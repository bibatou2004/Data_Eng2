from pyspark.sql import SparkSession, functions as F
import pathlib

# Initialisation
spark = SparkSession.builder.appName("DE2-Data-Augmentation").master("local[*]").getOrCreate()

# Dossiers
raw_dir = "data/raw"
aug_dir = "data/raw_augmented"
pathlib.Path(aug_dir).mkdir(parents=True, exist_ok=True)

# Fichiers stratégiques à traiter
files_to_check = ["match.csv", "players.csv", "chat.csv", "purchase_log.csv"]
dataframes = {}
total_rows = 0

print("--- 1. Comptage Initial ---")
for file in files_to_check:
    path = f"{raw_dir}/{file}"
    try:
        # inferSchema=True aide à faire des calculs mathématiques sur les clés
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(path)
        count = df.count()
        total_rows += count
        dataframes[file] = df
        print(f"{file} : {count:,} lignes")
    except Exception as e:
        print(f"⚠️ Erreur sur {file}. Le fichier est-il bien dans {raw_dir} ?")

print(f"\nTotal actuel : {total_rows:,} lignes")

# --- 2. Amplification (Data Augmentation) ---
TARGET_ROWS = 10_000_000

if total_rows > 0 and total_rows < TARGET_ROWS:
    multiplier = (TARGET_ROWS // total_rows) + 1
    print(f"\nObjectif non atteint. Multiplicateur appliqué : x{multiplier}")
    
    max_match_id_row = dataframes["match.csv"].agg(F.max("match_id")).collect()[0][0]
    max_match_id = int(max_match_id_row) if max_match_id_row else 10000000
        
    for file, df in dataframes.items():
        print(f"Amplification de {file}...")
        df_augmented = df
        
        for i in range(1, multiplier):
            df_temp = df.withColumn("match_id", F.col("match_id") + (i * max_match_id))
            
            if "start_time" in df.columns:
                df_temp = df_temp.withColumn("start_time", F.col("start_time") + (i * 2592000))
                
            df_augmented = df_augmented.unionByName(df_temp)
            
        out_path = f"{aug_dir}/{file.replace('.csv', '')}"
        df_augmented.coalesce(4).write.mode("overwrite").option("header", "true").csv(out_path)
        
        final_count = df_augmented.count()
        print(f"-> Terminé pour {file}. Nouveau volume : {final_count:,} lignes.")
        
    print("\n✅ Amplification terminée ! Tes données sont prêtes.")
    
elif total_rows >= TARGET_ROWS:
    print("\n✅ Objectif de 10 millions de lignes déjà atteint ! Tes données brutes suffisent.")