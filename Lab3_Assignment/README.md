# Lab3_Assignment: Data Engineering with Apache Spark

Analyse complète de données e-commerce avec **Spark SQL**, **DataFrames** et **RDDs**.

## 📋 Contenu du Lab

### Questions Q1-Q7
- **Q1**: Purchase price for specific session
- **Q2**: Products sold by brand "sokolov"
- **Q3**: Average purchase price by brand (Ferre)
- **Q4**: Average number of events per user
- **Q5**: Top 10 product-brand pairs by revenue
- **Q6**: Events by hour of day (with visualization)
- **Q7**: Average purchase price by brand > $10K (with bar chart)

### Section 5: RDD Operations
- Version 1: `groupByKey()` (naive algorithm)
- Version 3: `reduceByKey()` (optimized algorithm)
- Comparaison des performances

### Section 6: Join Implementations
- **Shuffle Join** (reduce-side join)
- **Replicated Hash Join** (broadcast join)

### Section 7: Performance Analysis
- Comparaison J1 (Shuffle) vs J2 (Hash: brands as R) vs J3 (Hash: products as R)
- Analyse des temps d'exécution

## 🚀 Setup

### Prérequis
- Apache Spark 4.0.1+
- Python 3.8+
- Jupyter Notebook

### Installation

```bash
# Cloner le repo
git clone https://github.com/bibatou2004/DataEng_Labs.git
cd Lab3_Assignment

# Installer les dépendances
pip install -r requirements.txt
```

### Télécharger les données

```bash
# Télécharger depuis Dropbox
wget https://www.dropbox.com/scl/fi/7012u693u06dgj95mgq2a/retail_dw_20250826.tar.gz

# Extraire
tar -xzf retail_dw_20250826.tar.gz -C data/input/
```

### Lancer le Notebook

```bash
jupyter notebook notebooks/Lab3_Assignment.ipynb
```

## 📊 Résultats Clés

| Question | Résultat | Type |
|----------|----------|------|
| Q1 | Purchase Price | SQL + DataFrame |
| Q2 | Num Products | SQL + DataFrame |
| Q3 | Avg Price | SQL + DataFrame |
| Q4 | Avg Events/User | SQL + DataFrame |
| Q5 | Top 10 Pairs | SQL + DataFrame |
| Q6 | Events by Hour | SQL + DataFrame + Plot |
| Q7 | Avg Price > 10K | SQL + DataFrame + Bar Chart |

## ⚡ Performance Comparison

### RDD Operations
- **V1 (groupByKey)**: Plus lent, shuffle complet
- **V3 (reduceByKey)**: Plus rapide, agrégation précoce

### Join Operations
- **J1 (Shuffle Join)**: Lent, shuffle global
- **J2 (Hash: R=brands)**: Moyen, broadcast petit
- **J3 (Hash: R=products)**: Rapide, broadcast grand

## 📁 Structure du Projet

```
Lab3_Assignment/
├── notebooks/
│   └── Lab3_Assignment.ipynb
├── data/
│   ├── input/         (parquet files)
│   └── output/        (résultats)
├── src/
│   ├── spark_config.py
│   ├── joins.py
│   └── utils.py
├── docs/
│   └── RESULTS.md
├── README.md
├── requirements.txt
└── .gitignore
```

## 📝 Notes Importantes

- Les fichiers parquet ne sont pas pushés (trop volumineux)
- Les visualisations matplotlib sont incluses dans le notebook
- Tous les résultats sont arrondis à 2 décimales

## 👤 Auteur

Biba Wanda Ogo

## 📄 Licence

MIT License
