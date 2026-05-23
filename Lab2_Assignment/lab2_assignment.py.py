#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Cell 0: Imports and Setup

import sys, os, subprocess, platform
from typing import Optional, Tuple
from pyspark.sql import DataFrame, SparkSession, functions as F, types as T, Window
from pyspark.sql.functions import col, to_date, to_timestamp
import time

# Check Python
print(f"Python: {sys.version}")

# Install psutil if needed
try:
    import psutil
except Exception:
    subprocess.check_call([sys.executable, "-m", "pip", "install", "psutil"])
    import psutil

print("✅ All imports loaded")


# In[2]:


# Cell 1: Initialize Spark Session

import findspark
findspark.init()

py = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = py
os.environ["PYSPARK_PYTHON"] = py

spark = SparkSession.getActiveSession() or (
    SparkSession.builder
    .appName("Lab2-ETL")
    .master("local[*]")
    .config("spark.driver.memory", "8g")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.pyspark.driver.python", py)
    .config("spark.pyspark.python", py)
    .getOrCreate()
)

print(f"✅ Spark {spark.version} initialized")
print(f"Master: {spark.sparkContext.master}")


# In[3]:


# Cell 2: Define paths and load CSV files

BASE_DIR = "/home/bibawandaogo/data engineering 1/lab2_data"

# Vérifie que les fichiers existent
import os
csv_files = ["user.csv", "session.csv", "brand.csv", "category.csv", 
             "product.csv", "product_name.csv", "events.csv"]

print("✅ Checking CSV files:")
for csv_file in csv_files:
    path = os.path.join(BASE_DIR, csv_file)
    exists = os.path.exists(path)
    size = os.path.getsize(path) if exists else 0
    print(f"   {csv_file}: {'✅' if exists else '❌'} ({size} bytes)")

print("\n✅ Loading DataFrames...")

# Charge tous les CSV
df_user = spark.read.option("header","true").option("inferSchema","true").csv(f"{BASE_DIR}/user.csv")
df_session = spark.read.option("header","true").option("inferSchema","true").csv(f"{BASE_DIR}/session.csv")
df_product = spark.read.option("header","true").option("inferSchema","true").csv(f"{BASE_DIR}/product.csv")
df_product_name = spark.read.option("header","true").option("inferSchema","true").csv(f"{BASE_DIR}/product_name.csv")
df_events = spark.read.option("header","true").option("inferSchema","true").csv(f"{BASE_DIR}/events.csv")
df_category = spark.read.option("header","true").option("inferSchema","true").csv(f"{BASE_DIR}/category.csv")
df_brand = spark.read.option("header","true").option("inferSchema","true").csv(f"{BASE_DIR}/brand.csv")

# Affiche les comptes
print("\n" + "=" * 60)
print("📊 Row Counts:")
print("=" * 60)
print(f"user:          {df_user.count()}")
print(f"session:       {df_session.count()}")
print(f"product:       {df_product.count()}")
print(f"product_name:  {df_product_name.count()}")
print(f"events:        {df_events.count()}")
print(f"category:      {df_category.count()}")
print(f"brand:         {df_brand.count()}")
print("=" * 60)


# In[4]:


# Cell 3: Build dim_user

print("✅ Building dim_user...")

# Ajoute la génération basée sur l'année de naissance
dim_user = (
    df_user
    .withColumn("birthdate", F.to_date(col("birthdate")))
    .withColumn("birth_year", F.year(col("birthdate")))
    .withColumn("generation", 
        F.when((col("birth_year") >= 1925) & (col("birth_year") <= 1945), "Traditionalists")
         .when((col("birth_year") >= 1946) & (col("birth_year") <= 1964), "Boomers")
         .when((col("birth_year") >= 1965) & (col("birth_year") <= 1980), "GenX")
         .when((col("birth_year") >= 1981) & (col("birth_year") <= 2000), "Millennials")
         .when((col("birth_year") >= 2001) & (col("birth_year") <= 2020), "GenZ")
         .otherwise("Unknown")
    )
    .withColumn("user_key", F.dense_rank().over(Window.orderBy(col("user_id"))))
    .select("user_key", "user_id", "gender", "birthdate", "generation")
)

print(f"✅ dim_user created with {dim_user.count()} rows")
dim_user.show(5)


# In[5]:


# Cell 4: Build dim_age

print("✅ Building dim_age...")

age_band_rows = [
    ("<18",   None, 17),
    ("18-24", 18, 24),
    ("25-34", 25, 34),
    ("35-44", 35, 44),
    ("45-54", 45, 54),
    ("55-64", 55, 64),
    ("65-74", 65, 74),
    ("75-84", 75, 84),
    ("85-94", 85, 94),
    ("unknown", None, None),
]

dim_age = spark.createDataFrame(age_band_rows, ["age_band", "min_age", "max_age"])
w_age = Window.orderBy(F.col("age_band"))
dim_age = dim_age.withColumn("age_key", F.dense_rank().over(w_age))
dim_age = dim_age.select("age_key", "age_band", "min_age", "max_age")

print(f"✅ dim_age created with {dim_age.count()} rows")
dim_age.show()


# In[6]:


# Cell 5: Build dim_brand

print("✅ Building dim_brand...")

dim_brand = (
    df_brand
    .withColumn("brand_key", F.dense_rank().over(Window.orderBy(col("brand"))))
    .select("brand_key", F.col("brand").alias("brand_code"), F.col("description").alias("brand_desc"))
)

print(f"✅ dim_brand created with {dim_brand.count()} rows")
dim_brand.show()


# In[7]:


# Cell 6: Build dim_category

print("✅ Building dim_category...")

dim_category = (
    df_category
    .withColumn("category_key", F.dense_rank().over(Window.orderBy(col("category"))))
    .select("category_key", F.col("category").alias("category_code"), F.col("description").alias("category_desc"))
)

print(f"✅ dim_category created with {dim_category.count()} rows")
dim_category.show()


# In[8]:


# Cell 7: Build dim_product

print("✅ Building dim_product...")

# Join product with product_name pour ajouter les descriptions
df_product_enriched = (
    df_product
    .join(df_product_name, on=["category", "product_name"], how="left")
    .select("product_id", "brand", "category", "product_name", "description")
)

# Join avec dim_brand pour obtenir brand_key
df_product_with_brand = (
    df_product_enriched
    .join(dim_brand.select("brand_key", "brand_code"), 
          df_product_enriched.brand == dim_brand.brand_code, 
          how="left")
    .select(
        F.col("product_id"),
        F.col("product_name").alias("product_desc"),
        F.col("brand_key")
    )
)

# Join avec dim_category pour obtenir category_key
dim_product = (
    df_product_enriched
    .join(dim_category.select("category_key", "category_code"), 
          df_product_enriched.category == dim_category.category_code, 
          how="left")
    .join(dim_brand.select("brand_key", "brand_code"), 
          df_product_enriched.brand == dim_brand.brand_code, 
          how="left")
    .withColumn("product_key", F.dense_rank().over(Window.orderBy(col("product_id"))))
    .select("product_key", "product_id", F.col("description").alias("product_desc"), "brand_key", "category_key")
)

print(f"✅ dim_product created with {dim_product.count()} rows")
dim_product.show()


# In[9]:


# Cell 8: Build dim_date

print("✅ Building dim_date...")

# Convertis event_time en date
df_events_with_date = df_events.withColumn("event_date", F.to_date(F.col("event_time")))

# Trouve la plage de dates
min_date = df_events_with_date.agg(F.min("event_date")).collect()[0][0]
max_date = df_events_with_date.agg(F.max("event_date")).collect()[0][0]

print(f"Date range: {min_date} to {max_date}")

# Génère toutes les dates dans la plage
dim_date = (
    spark.sql(f"SELECT explode(sequence(to_date('{min_date}'), to_date('{max_date}'))) as date")
    .withColumn("year", F.year(F.col("date")))
    .withColumn("month", F.month(F.col("date")))
    .withColumn("day", F.dayofmonth(F.col("date")))
    .withColumn("day_of_week", F.dayofweek(F.col("date")))
    .withColumn("day_name", F.date_format(F.col("date"), "EEEE"))
    .withColumn("is_weekend", (F.col("day_of_week") == 1) | (F.col("day_of_week") == 7))
    .withColumn("week_of_year", F.weekofyear(F.col("date")))
    .withColumn("month_name", F.date_format(F.col("date"), "MMMM"))
    .withColumn("quarter", F.quarter(F.col("date")))
    .withColumn("date_key", F.col("year") * 10000 + F.col("month") * 100 + F.col("day"))
    .select("date_key", "date", "day", "day_of_week", "day_name", "is_weekend", 
            "week_of_year", "month", "month_name", "quarter", "year")
)

print(f"✅ dim_date created with {dim_date.count()} rows")
dim_date.show()


# In[10]:


# Cell 9: Summary of all dimensions

print("\n" + "=" * 60)
print("📊 DIMENSION TABLES SUMMARY")
print("=" * 60)
print(f"dim_user:      {dim_user.count()} rows")
print(f"dim_age:       {dim_age.count()} rows")
print(f"dim_brand:     {dim_brand.count()} rows")
print(f"dim_category:  {dim_category.count()} rows")
print(f"dim_product:   {dim_product.count()} rows")
print(f"dim_date:      {dim_date.count()} rows")
print("=" * 60)

# Affiche les schémas
print("\n✅ Schemas:")
print("\ndim_user:")
dim_user.printSchema()
print("\ndim_product:")
dim_product.printSchema()
print("\ndim_date:")
dim_date.printSchema()


# In[11]:


# Cell 10: Clean Events

print("✅ Cleaning events...")

# Convertis event_time en timestamp
df_events_clean = (
    df_events
    .withColumn("event_time", F.to_timestamp(col("event_time")))
    .withColumn("event_date", F.to_date(F.col("event_time")))
    .withColumn("price", F.col("price").cast("double"))
)

# Filtre les événements invalides
valid_types = ["view", "cart", "purchase", "remove"]

events_clean = (
    df_events_clean
    .filter(F.col("event_time").isNotNull())
    .filter(F.col("session_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter((F.col("price").isNull()) | (F.col("price") >= 0))
    .filter(F.col("event_type").isin(valid_types))
    .filter(F.col("event_date") <= F.current_date())
)

print(f"✅ events_clean: {events_clean.count()} rows")
events_clean.show(5)


# In[12]:


# Cell 11: Analyze prices

print("✅ Price Statistics...")

price_stats = events_clean.agg(
    F.min("price").alias("minimum"),
    F.max("price").alias("maximum"),
    F.avg("price").alias("average"),
    F.count("price").alias("count_non_null")
).collect()[0]

minimum = price_stats["minimum"]
maximum = price_stats["maximum"]
average = price_stats["average"]

print(f"Minimum price: {minimum}")
print(f"Maximum price: {maximum}")
print(f"Average price: {average:.2f}")
print(f"Non-null prices: {price_stats['count_non_null']}")

# Calcule le threshold: 100x la moyenne
threshold = (average or 0) * 100
print(f"\n🔍 Price threshold (100x average): {threshold:.2f}")

# Filtre les prix excessifs
events_clean = events_clean.filter(
    (F.col("price").isNull()) | (F.col("price") <= threshold)
)

print(f"✅ After filtering expensive items: {events_clean.count()} rows")


# In[13]:


# Cell 12: Create lookup tables

print("✅ Creating lookup tables...")

# Lookup: user_id → user_key
user_lkp = dim_user.select("user_id", "user_key")

# Lookup: product_id → product_key, brand_key, category_key
prod_lkp = dim_product.select("product_id", "product_key", "brand_key", "category_key")

# Lookup: date → date_key
date_lkp = dim_date.select("date", "date_key")

# Bridge: session_id → user_id
session_bridge = df_session.select("session_id", "user_id")

print(f"user_lkp: {user_lkp.count()}")
print(f"prod_lkp: {prod_lkp.count()}")
print(f"date_lkp: {date_lkp.count()}")
print(f"session_bridge: {session_bridge.count()}")


# In[14]:


# Cell 13: Build fact_events

print("✅ Building fact_events...")

# Démarre avec les événements nettoyés
fact_events = events_clean.select(
    "event_time", "event_type", "session_id", "product_id", "price", "event_date"
)

# Join 1: Récupère user_id via session_id
fact_events = (
    fact_events
    .join(session_bridge, on="session_id", how="left")
)

# Join 2: Récupère product_key, brand_key, category_key
fact_events = (
    fact_events
    .join(prod_lkp, on="product_id", how="left")
)

# Join 3: Récupère date_key
fact_events = (
    fact_events
    .join(date_lkp, fact_events.event_date == date_lkp.date, how="left")
    .drop("date")
)

# Join 4: Récupère user_key et birthdate
fact_events = (
    fact_events
    .join(user_lkp, on="user_id", how="left")
    .join(dim_user.select("user_key", "birthdate"), on="user_key", how="left")
)

# Calcule l'âge au moment de l'événement
fact_events = fact_events.withColumn(
    "age_on_event", 
    F.floor(F.months_between(F.col("event_date"), F.to_date("birthdate"))/12)
)

# Join 5: Récupère age_key basé sur age_on_event
fact_events = (
    fact_events
    .join(
        dim_age.select("age_key", "age_band", "min_age", "max_age"),
        (
            ((F.col("age_on_event") > F.col("min_age"))) &
            ((F.col("age_on_event") <= F.col("max_age")))
        ),
        "left"
    )
)

# Sélectionne les colonnes finales
fact_events = fact_events.select(
    "date_key",
    "user_key",
    "age_key",
    "product_key",
    "brand_key",
    "category_key",
    "session_id",
    "event_time",
    "event_type",
    "price"
)

print(f"✅ fact_events created with {fact_events.count()} rows")
fact_events.show(10)


# In[15]:


# Cell 14: Display fact_events details

print("\n" + "=" * 70)
print("📊 FACT_EVENTS TABLE")
print("=" * 70)

fact_events.printSchema()

print(f"\nTotal rows: {fact_events.count()}")
print("\nSample data:")
fact_events.show(10, truncate=False)

print("\n" + "=" * 70)
print("✅ STAR SCHEMA COMPLETE!")
print("=" * 70)


# In[16]:


# Cell 15: Quality Gates

print("\n" + "=" * 70)
print("🔍 QUALITY GATES")
print("=" * 70)

# Gate 1: Verify row count is non-zero
gate_1_count = fact_events.count()
gate_1_pass = gate_1_count > 0

print(f"\n✅ GATE 1: Row count non-zero")
print(f"   Rows: {gate_1_count}")
print(f"   Status: {'✅ PASS' if gate_1_pass else '❌ FAIL'}")

if not gate_1_pass:
    raise Exception("GATE 1 FAILED: No rows in fact_events!")

# Gate 2: Check null rate thresholds
print(f"\n✅ GATE 2: Null rate thresholds")

null_checks = {
    "date_key": 0.05,      # Max 5% nulls
    "user_key": 0.05,      # Max 5% nulls
    "product_key": 0.05,   # Max 5% nulls
    "event_type": 0.01,    # Max 1% nulls
    "price": 0.20,         # Max 20% nulls (views don't have prices)
}

gate_2_pass = True
for col_name, threshold in null_checks.items():
    null_count = fact_events.filter(F.col(col_name).isNull()).count()
    null_rate = null_count / gate_1_count
    
    passed = null_rate <= threshold
    gate_2_pass = gate_2_pass and passed
    
    status = "✅" if passed else "❌"
    print(f"   {status} {col_name}: {null_rate:.2%} (threshold: {threshold:.2%})")

print(f"   Status: {'✅ PASS' if gate_2_pass else '❌ FAIL'}")

if not gate_2_pass:
    raise Exception("GATE 2 FAILED: Null rate threshold exceeded!")

# Gate 3: Referential integrity checks (FK coverage)
print(f"\n✅ GATE 3: Referential integrity (FK coverage)")

# Check date_key references
date_keys_in_fact = set(fact_events.select("date_key").rdd.flatMap(lambda x: x).collect())
date_keys_in_dim = set(dim_date.select("date_key").rdd.flatMap(lambda x: x).collect())
missing_dates = date_keys_in_fact - date_keys_in_dim

# Check user_key references
user_keys_in_fact = set(fact_events.filter(F.col("user_key").isNotNull()).select("user_key").rdd.flatMap(lambda x: x).collect())
user_keys_in_dim = set(dim_user.select("user_key").rdd.flatMap(lambda x: x).collect())
missing_users = user_keys_in_fact - user_keys_in_dim

# Check product_key references
product_keys_in_fact = set(fact_events.filter(F.col("product_key").isNotNull()).select("product_key").rdd.flatMap(lambda x: x).collect())
product_keys_in_dim = set(dim_product.select("product_key").rdd.flatMap(lambda x: x).collect())
missing_products = product_keys_in_fact - product_keys_in_dim

gate_3_pass = (len(missing_dates) == 0) and (len(missing_users) == 0) and (len(missing_products) == 0)

print(f"   Date references: {len(missing_dates)} missing")
print(f"   User references: {len(missing_users)} missing")
print(f"   Product references: {len(missing_products)} missing")
print(f"   Status: {'✅ PASS' if gate_3_pass else '❌ FAIL'}")

if not gate_3_pass:
    raise Exception("GATE 3 FAILED: Referential integrity broken!")

# Final verdict
print("\n" + "=" * 70)
all_gates_pass = gate_1_pass and gate_2_pass and gate_3_pass
if all_gates_pass:
    print("✅ ALL QUALITY GATES PASSED!")
else:
    print("❌ SOME GATES FAILED - CHECK ABOVE")
print("=" * 70)


# In[17]:


# Cell 16: Export to CSV and Parquet

import os
import shutil

OUTPUT_DIR = "/home/bibawandaogo/data engineering 1/lab2_output"

# Crée le répertoire de sortie
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("\n" + "=" * 70)
print("📤 EXPORTING OUTPUTS")
print("=" * 70)

# 1. CSV (uncompressed)
print("\n✅ Writing CSV (no compression)...")
csv_uncompressed = f"{OUTPUT_DIR}/fact_events_csv"
if os.path.exists(csv_uncompressed):
    shutil.rmtree(csv_uncompressed)

fact_events.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_uncompressed)
print(f"   ✅ Saved to {csv_uncompressed}")

# 2. CSV (Snappy compressed)
print("\n✅ Writing CSV (Snappy compressed)...")
csv_snappy = f"{OUTPUT_DIR}/fact_events_csv_snappy"
if os.path.exists(csv_snappy):
    shutil.rmtree(csv_snappy)

fact_events.coalesce(1).write.mode("overwrite").option("header", "true").option("compression", "snappy").csv(csv_snappy)
print(f"   ✅ Saved to {csv_snappy}")

# 3. Parquet (default compression)
print("\n✅ Writing Parquet...")
parquet_path = f"{OUTPUT_DIR}/fact_events_parquet"
if os.path.exists(parquet_path):
    shutil.rmtree(parquet_path)

fact_events.coalesce(1).write.mode("overwrite").parquet(parquet_path)
print(f"   ✅ Saved to {parquet_path}")

print("\n" + "=" * 70)


# In[18]:


# Cell 17: Compare file sizes

import os

print("\n" + "=" * 70)
print("📊 FILE SIZE COMPARISON")
print("=" * 70)

def get_dir_size(path):
    """Calcule la taille totale d'un répertoire en MB"""
    total = 0
    for dirpath, dirnames, filenames in os.walk(path):
        for filename in filenames:
            filepath = os.path.join(dirpath, filename)
            if os.path.exists(filepath):
                total += os.path.getsize(filepath)
    return total / (1024 * 1024)  # Convert to MB

output_paths = {
    "CSV (uncompressed)": f"{OUTPUT_DIR}/fact_events_csv",
    "CSV (Snappy)": f"{OUTPUT_DIR}/fact_events_csv_snappy",
    "Parquet": f"{OUTPUT_DIR}/fact_events_parquet",
}

sizes = {}
for name, path in output_paths.items():
    try:
        size_mb = get_dir_size(path)
        sizes[name] = size_mb
        print(f"\n{name}:")
        print(f"   Size: {size_mb:.4f} MB")
    except Exception as e:
        print(f"\n{name}: Error - {e}")

# Calcule les ratios
if "Parquet" in sizes and sizes["Parquet"] > 0:
    csv_ratio = sizes.get("CSV (uncompressed)", 0) / sizes["Parquet"]
    snappy_ratio = sizes.get("CSV (Snappy)", 0) / sizes["Parquet"]
    
    print("\n" + "=" * 70)
    print("📈 COMPRESSION RATIOS (vs Parquet)")
    print("=" * 70)
    print(f"CSV vs Parquet:          {csv_ratio:.1f}x larger")
    print(f"CSV Snappy vs Parquet:   {snappy_ratio:.1f}x larger")
    print("=" * 70)

print(f"\n✅ Total data output: {sum(sizes.values()):.4f} MB")


# In[19]:


# Cell 18: Spark Execution Plans

print("\n" + "=" * 70)
print("📋 SPARK EXECUTION PLANS")
print("=" * 70)

print("\n✅ Transform Plan (events_clean):")
print("-" * 70)
events_clean.explain(mode="formatted")

print("\n\n✅ Join & Aggregate Plan (fact_events):")
print("-" * 70)
fact_events.explain(mode="formatted")


# In[20]:


# Cell 19: Final Summary

print("\n\n" + "=" * 70)
print("🎉 LAB 2 ASSIGNMENT - COMPLETE SUMMARY")
print("=" * 70)

print("\n📊 DATA WAREHOUSE STAR SCHEMA:")
print("-" * 70)
print(f"dim_user:      {dim_user.count():>6} rows  | FK in fact_events")
print(f"dim_age:       {dim_age.count():>6} rows  | FK in fact_events")
print(f"dim_brand:     {dim_brand.count():>6} rows  | FK in fact_events")
print(f"dim_category:  {dim_category.count():>6} rows  | FK in fact_events")
print(f"dim_product:   {dim_product.count():>6} rows  | FK in fact_events")
print(f"dim_date:      {dim_date.count():>6} rows  | FK in fact_events")
print(f"{'─' * 70}")
print(f"fact_events:   {fact_events.count():>6} rows  | Main fact table")

print("\n🔍 QUALITY GATES:")
print("-" * 70)
print("✅ Gate 1: Row count non-zero")
print("✅ Gate 2: Null rate thresholds")
print("✅ Gate 3: Referential integrity")

print("\n💾 OUTPUTS:")
print("-" * 70)
print(f"CSV uncompressed: {sizes.get('CSV (uncompressed)', 0):.4f} MB")
print(f"CSV Snappy:       {sizes.get('CSV (Snappy)', 0):.4f} MB")
print(f"Parquet:          {sizes.get('Parquet', 0):.4f} MB")

print("\n⚙️ SPARK CONFIG:")
print("-" * 70)
print(f"Version:                {spark.version}")
print(f"Master:                 {spark.sparkContext.master}")
print(f"Driver Memory:          8g")
print(f"Shuffle Partitions:     200")
print(f"Adaptive Execution:     Enabled")

print("\n📝 KEY INSIGHTS:")
print("-" * 70)
print("1. Parquet is much smaller than CSV formats")
print("   → Columnar storage compresses better")
print("   → Better for analytical queries")
print("\n2. Quality gates ensure data integrity")
print("   → All foreign keys validated")
print("   → Null rates within thresholds")
print("\n3. Built-in functions used (no UDFs)")
print("   → F.months_between for age calculation")
print("   → F.dense_rank for surrogate keys")
print("   → Better performance than custom code")

print("\n" + "=" * 70)
print("✅ ALL TASKS COMPLETED SUCCESSFULLY!")
print("=" * 70)

