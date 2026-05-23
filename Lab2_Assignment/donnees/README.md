# 📊 Dossier Données Lab 2

## 📂 Structure

```
donnees/
├── README.md              # Ce fichier
├── MANIFEST.md            # Détails des fichiers
├── entrees/               # Données source (CSV)
│   ├── user.csv
│   ├── session.csv
│   ├── product.csv
│   ├── product_name.csv
│   ├── events.csv
│   ├── brand.csv
│   └── category.csv
│
└── sorties/               # Résultats du pipeline ETL
    ├── fact_events_csv/           # Export CSV brut
    ├── fact_events_csv_snappy/    # Export CSV compressé
    └── fact_events_parquet/       # Export Parquet (recommandé)
```

## �� Données d'Entrée

7 fichiers CSV sources (~2 KB total):

- **user.csv**: 10 utilisateurs
- **session.csv**: Sessions utilisateurs
- **product.csv**: 10 produits
- **product_name.csv**: Descriptions produits
- **events.csv**: 20 événements e-commerce
- **brand.csv**: 5 marques
- **category.csv**: 5 catégories

Voir [MANIFEST.md](MANIFEST.md) pour détails complets.

## 📤 Données de Sortie

Table de faits `fact_events` exportée en 3 formats:

### CSV Brut
```bash
fact_events_csv/
└── part-00000-*.csv  (0.0010 MB)
```

### CSV Snappy Compressé
```bash
fact_events_csv_snappy/
└── part-00000-*.csv.snappy  (0.0008 MB)
```

### Parquet (Recommandé!)
```bash
fact_events_parquet/
└── part-00000-*.parquet  (0.0005 MB)
```

## 🚀 Comment Utiliser

### Charger les données d'entrée
```python
import pyspark.sql.functions as F
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Lab2").getOrCreate()

df_user = spark.read.csv("entrees/user.csv", header=True, inferSchema=True)
df_events = spark.read.csv("entrees/events.csv", header=True, inferSchema=True)
```

### Charger les sorties
```python
# CSV
fact_csv = spark.read.csv("sorties/fact_events_csv/", header=True)

# Parquet (recommandé)
fact_parquet = spark.read.parquet("sorties/fact_events_parquet/")
```

## 📊 Statistiques Clés

- **Total entrées**: ~2 KB
- **Total sorties**: ~0.0023 MB
- **Compression**: Parquet 2x plus petit que CSV
- **Événements traités**: 20
- **Qualité**: 100% validée ✅

## 🔍 Qualité des Données

### Porte 1: Comptage ✅
- fact_events: 20 lignes (> 0) ✅

### Porte 2: Taux Nullité ✅
- date_key: 0.00% ≤ 5% ✅
- user_key: 0.00% ≤ 5% ✅
- product_key: 0.00% ≤ 5% ✅

### Porte 3: Intégrité Référentielle ✅
- FK date_key: 0 manquants ✅
- FK user_key: 0 manquants ✅
- FK product_key: 0 manquants ✅

---

Pour plus de détails, voir [MANIFEST.md](MANIFEST.md)
