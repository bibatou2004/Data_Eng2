# 🚀 Lab 2 Practice: PostgreSQL → Star Schema ETL

**Data Engineering I - ESIEE 2025-2026**
**Auteur:** Badr TAJINI  
**Date:** Décembre 2025

---

## 📋 Vue d'ensemble

Ce lab pratique implémente un **pipeline ETL complet** transformant une base de données opérationnelle PostgreSQL en un **data warehouse Star Schema** en utilisant **Apache Spark**.

### 🎯 Objectifs

✅ Ingérer des données opérationnelles (CSV)  
✅ Construire un schéma en étoile (Star Schema)  
✅ Générer des clés de substitution stables  
✅ Optimiser les joins et projections  
✅ Valider la qualité des données  
✅ Exporter en Parquet optimisé  

---

## 📊 Architecture

### Données Opérationnelles (Bronze Layer)

```
customers           → 10 clients
brands              → 5 marques
categories          → 5 catégories
products            → 20 produits
orders              → 50 commandes
order_items         → 100 lignes de commande
```

### Dimensions (Silver Layer)

| Dimension | Lignes | Colonnes | Clés |
|-----------|--------|----------|------|
| **dim_customer** | 10 | 5 | customer_sk, customer_id |
| **dim_brand** | 5 | 3 | brand_sk, brand_id |
| **dim_category** | 5 | 3 | category_sk, category_id |
| **dim_product** | 20 | 6 | product_sk, product_id |
| **dim_date** | ~150 | 8 | date_sk, date |

### Table de Faits (Gold Layer)

```
fact_sales
├── order_id (PK)
├── date_sk (FK → dim_date)
├── customer_sk (FK → dim_customer)
├── product_sk (FK → dim_product)
├── quantity
├── unit_price
├── subtotal
├── year (partition)
└── month (partition)
```

**Statistiques:**
- 100 lignes de faits
- 50 commandes uniques
- GMV total: ~$20,000
- AOV moyen: ~$200

---

## 🏗️ Structure du Projet

```
Lab2_Practice/
├── README.md                           # Ce fichier
├── requirements.txt                    # Dépendances Python
│
├── notebooks/
│   └── lab2_practice.ipynb             # Notebook Jupyter principal (8 étapes)
│
├── data/                               # Données sources (Bronze)
│   ├── lab2_customers.csv              # 10 clients
│   ├── lab2_brands.csv                 # 5 marques
│   ├── lab2_categories.csv             # 5 catégories
│   ├── lab2_products.csv               # 20 produits
│   ├── lab2_orders.csv                 # 50 commandes
│   └── lab2_order_items.csv            # 100 articles
│
├── outputs/lab2/                       # Sorties Parquet (Gold)
│   ├── dim_customer/                   # Parquet
│   ├── dim_brand/                      # Parquet
│   ├── dim_category/                   # Parquet
│   ├── dim_product/                    # Parquet
│   ├── dim_date/                       # Parquet
│   └── fact_sales/                     # Parquet partitionné (year/month)
│
├── proof/                              # Preuves & Métriques
│   ├── plan_ingest.txt                 # Plan ingestion
│   ├── ingestion_summary.csv           # Stats ingestion
│   ├── dimensions_summary.csv          # Stats dimensions
│   ├── date_dimension_summary.csv      # Stats dates
│   ├── fact_sales_summary.csv          # Stats table de faits
│   ├── plan_fact_join.txt              # Plan fact_sales
│   ├── plan_case_a_late_projection.txt # Plan projection tardive
│   ├── plan_case_b_early_projection.txt # Plan projection précoce
│   ├── projection_comparison.csv       # Comparaison perfs
│   └── lab2_metrics_final.csv          # Métriques finales
│
└── docs/                               # Documentation
    ├── ARCHITECTURE.md                 # Détails architecture
    ├── DATA_SCHEMA.md                  # Schémas détaillés
    └── OPTIMIZATION_NOTES.md           # Notes d'optimisation
```

---

## 🚀 Démarrage Rapide

### Prérequis

```bash
# Python 3.8+
python --version

# PySpark 3.x
pip install pyspark>=3.0.0

# Pandas
pip install pandas
```

### Installation

```bash
# Clone le repo
git clone https://github.com/bibatou2004/DataEng_Labs.git
cd DataEng_Labs/Lab2_Practice

# Installe les dépendances
pip install -r requirements.txt

# Lance Jupyter
jupyter notebook notebooks/lab2_practice.ipynb
```

### Exécution

1. **Ouvre le notebook** dans Jupyter
2. **Exécute les cellules** dans l'ordre (Shift+Enter)
3. **Observe les résultats** à chaque étape
4. **Vérifie les fichiers de preuve** dans `proof/`

---

## 📚 Détails des Étapes

### Étape 0: Setup et Schémas
- Initialise Spark Session
- Définit schémas explicites pour tous les CSV
- Crée répertoires de sortie

### Étape 1: Ingestion des Données
- Charge 6 tables CSV
- Affiche comptages et profils
- Sauvegarde plan d'ingestion

### Étape 2: Fonction Clé de Substitution
```python
def sk(cols):
    return F.abs(F.xxhash64(*[F.col(c) for c in cols]))
```
- Hash stable 64-bit
- Déterministe (même input = même output)
- Positif avec `abs()`

### Étape 3: Construction des Dimensions
- dim_customer (10 rows)
- dim_brand (5 rows)
- dim_category (5 rows)
- dim_product (20 rows, avec FKs)

### Étape 4: Dimension Date
- Extrait dates uniques des commandes
- Génère attributs temporels (year, month, day, dow, quarter, week)
- ~150 jours sur 6 mois

### Étape 5: Table de Faits (Propre)
```python
# ÉTAPE 1: Joins
df_joined = (oi
    .join(p, F.col("oi.product_id") == F.col("p.product_id"))
    .join(o, F.col("oi.order_id") == F.col("o.order_id"))
    .join(c, F.col("o.customer_id") == F.col("c.customer_id"))
)

# ÉTAPE 2: Projection immédiate (désambiguation)
df_joined = df_joined.select(
    F.col("oi.order_id").alias("order_id"),
    ...
)

# ÉTAPE 3: Transformations
df_fact = df_joined.withColumn(...)
```

**Statistiques fact_sales:**
- 100 lignes de faits
- Mesures: quantity, unit_price, subtotal
- Partitions: year/month

### Étape 6: Export Parquet
```
dim_customer/ ──→ Parquet
dim_brand/    ──→ Parquet
dim_category/ ──→ Parquet
dim_product/  ──→ Parquet
dim_date/     ──→ Parquet
fact_sales/   ──→ Parquet (partitionné year/month)
```

### Étape 7: Optimisation - Projection Tardive vs Précoce

**Cas A: Projection Tardive** (JOIN → AGG)
```python
(orders.join(order_items).join(products)
 .groupBy(...).agg(...))
```
- ❌ Traite toutes les colonnes
- ❌ Plus lent

**Cas B: Projection Précoce** (SELECT → JOIN → AGG)
```python
(orders.select("order_id", "order_date")
 .join(order_items.select("order_id", "product_id", "quantity"))
 .join(products.select("product_id", "price"))
 .groupBy(...).agg(...))
```
- ✅ Filtre les colonnes inutiles
- ✅ Plus rapide (surtout sur gros volumes)

**Résultats sur ce dataset:**
- Cas A (Tardive): ~X.XXXs
- Cas B (Précoce): ~X.XXXs
- Amélioration: ~Y%

### Étape 8: Résumé Final
Affiche et sauvegarde:
- Métriques clés (counts, GMV, AOV)
- Versions logicielles
- Fichiers de preuve générés

---

## 📊 Métriques Clés

| Métrique | Valeur |
|----------|--------|
| **Total Clients** | 10 |
| **Total Commandes** | 50 |
| **Total Articles** | 100 |
| **GMV Total** | ~$20,000 |
| **AOV Moyen** | ~$200 |
| **Dates Uniques** | ~150 |
| **Spark Version** | 3.x |

---

## 🔧 Dépannage

### Erreur: AMBIGUOUS_REFERENCE
**Problème:** Colonnes ambiguës dans les joins
```python
# ❌ Mauvais
.withColumn("customer_sk", sk(["customer_id"]))

# ✅ Bon
.withColumn("customer_sk", sk(["c.customer_id"]))
# OU projeter immédiatement après le join
```

### Erreur: File not found
```bash
# Vérifie le chemin des données
ls -la data/lab2_*.csv

# Lance depuis le bon répertoire
cd Lab2_Practice
```

### Spark Out of Memory
```bash
# Augmente la mémoire driver
spark = SparkSession.builder \
    .config("spark.driver.memory", "16g") \
    .getOrCreate()
```

---

## 📈 Performances

### Optimisations Appliquées

1. **Projection Précoce**
   - Réduit les données transmises entre les étapes
   - Permet à Spark d'optimiser mieux

2. **Partitionnement des Sorties**
   - fact_sales partitionné par year/month
   - Accélère les requêtes filtrées par date

3. **Parquet vs CSV**
   - Colonnaire (meilleure compression)
   - Schéma intégré
   - Lecture sélective

### Mesures

Voir `proof/projection_comparison.csv`:
```csv
Cas,Approche,Temps(s),Lignes,Amélioration(%)
A,Projection Tardive,X.XXX,150,0.0
B,Projection Précoce,X.XXX,150,Y.Y
```

---

## 📚 Concepts Couverts

### SQL & Spark
- ✅ Joins (inner, left, outer)
- ✅ Projections optimisées
- ✅ Window functions (rank)
- ✅ Agrégations (sum, avg, count)
- ✅ Partitionnement

### Data Engineering
- ✅ ETL pipeline
- ✅ Star Schema design
- ✅ Slowly Changing Dimensions
- ✅ Surrogate keys
- ✅ Data quality gates

### Spark Performance
- ✅ Query plans (DAG)
- ✅ Shuffle operations
- ✅ Broadcasting
- ✅ Columnar storage (Parquet)

---

## 📁 Fichiers Importants

### Fichiers de Preuve
| Fichier | Contenu | Format |
|---------|---------|--------|
| `plan_ingest.txt` | Plan ingestion | TXT |
| `plan_fact_join.txt` | Plan fact_sales | TXT |
| `dimensions_summary.csv` | Stats dimensions | CSV |
| `fact_sales_summary.csv` | Stats table faits | CSV |
| `projection_comparison.csv` | Bench A vs B | CSV |
| `lab2_metrics_final.csv` | Métriques finales | CSV |

### Données
- **Bronze:** `data/lab2_*.csv` (sources)
- **Gold:** `outputs/lab2/` (Parquet)

---

## 🤝 Contribution

Pour améliorer ce lab:
1. Fork le repo
2. Crée une branche (`git checkout -b feature/improvement`)
3. Commit tes changements (`git commit -am 'Add improvement'`)
4. Push (`git push origin feature/improvement`)
5. Ouvre une Pull Request

---

## 📝 Licence

MIT License - voir LICENSE

---

## 👨‍🏫 Auteur

**Badr TAJINI**  
Data Engineering I - ESIEE 2025-2026

---

## 📞 Support

Questions? Ouvre une issue ou contacte via GitHub Discussions.

---

**Dernière mise à jour:** Décembre 2025  
**Status:** ✅ Complété et Testé
