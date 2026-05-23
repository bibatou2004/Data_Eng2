# Rapport Lab 2: Data Warehouse ETL

## 📋 Résumé Exécutif

✅ Construction réussie d'un entrepôt de données avec architecture Star Schema.

**Statistiques:**
- 6 tables de dimension
- 1 table de faits
- 20 événements
- 3 formats d'export
- 100% portes de qualité validées

## 📊 Architecture Finale

### Schéma en Étoile
```
                    dim_utilisateur
                           ↑
                           │
    dim_date ← fact_events → dim_produit
    dim_age ↗         ↘ dim_categorie
                    dim_marque
```

### Dimensions

#### dim_utilisateur (10 rows)
- user_key (PK)
- user_id (FK métier)
- gender
- birthdate
- generation (Traditionalists/Boomers/GenX/Millennials/GenZ)

#### dim_age (10 rows)
- age_key (PK)
- age_band (<18, 18-24, 25-34, ..., 85-94, unknown)
- min_age, max_age

#### dim_marque (5 rows)
- brand_key (PK)
- brand_code
- brand_desc

#### dim_categorie (5 rows)
- category_key (PK)
- category_code
- category_desc

#### dim_produit (10 rows)
- product_key (PK)
- product_id
- product_desc
- brand_key (FK)
- category_key (FK)

#### dim_date (4 rows)
- date_key (PK) [YYYYMMDD]
- date
- year, month, day
- day_of_week, day_name
- is_weekend
- week_of_year, month_name
- quarter

### Table de Faits

**fact_events** (20 rows)
| Colonne | Type | FK |
|---------|------|-----|
| date_key | INT | ✅ |
| utilisateur_key | INT | ✅ |
| age_key | INT | ✅ |
| produit_key | INT | ✅ |
| marque_key | INT | ✅ |
| categorie_key | INT | ✅ |
| session_id | STRING | - |
| event_time | TIMESTAMP | - |
| event_type | STRING | - |
| price | DOUBLE | - |

## ✅ Portes de Qualité

### Porte 1: Comptage Non-Zéro ✅
```
Condition: COUNT(fact_events) > 0
Résultat: 20 > 0
Statut: ✅ PASS
```

### Porte 2: Taux de Nullité ✅
```
date_key:     0.00% ≤ 5.00%  ✅
utilisateur_key: 0.00% ≤ 5.00%  ✅
produit_key:  0.00% ≤ 5.00%  ✅
event_type:   0.00% ≤ 1.00%  ✅
price:        0.00% ≤ 20.00% ✅
```

### Porte 3: Intégrité Référentielle ✅
```
FK date_key → dim_date:         0 manquants ✅
FK utilisateur_key → dim_utilisateur: 0 manquants ✅
FK produit_key → dim_produit:   0 manquants ✅
```

## 📈 Compression & Performance

### Comparaison Tailles de Fichiers
| Format | Taille | Ratio vs Parquet |
|--------|--------|------------------|
| CSV brut | 0.0010 MB | 2.0x |
| CSV Snappy | 0.0008 MB | 1.6x |
| **Parquet** | **0.0005 MB** | **1.0x** |

**Conclusion:** Parquet est 2x plus compact que CSV!

## 🛠️ Transformations Appliquées

### Nettoyage events
```python
✅ Suppression timestamps NULL
✅ Suppression session_id NULL
✅ Suppression product_id NULL
✅ Filtre prix négatifs
✅ Filtre dates futures
✅ Validation event_types
✅ Suppression prix outliers (>100x moyenne)
```

### Enrichissement Dimensions
```python
dim_utilisateur:
  + Calcul year_of_birth
  + Classification génération (basée année naissance)
  + Génération user_key (dense_rank)

dim_produit:
  + JOIN product_name (descriptions)
  + JOIN dim_marque (brand_key)
  + JOIN dim_categorie (categorie_key)
  + Génération product_key
```

### Construction fact_events
```python
events_clean
  → JOIN session_bridge (session_id → utilisateur_id)
  → JOIN prod_lkp (produit_id → clés)
  → JOIN date_lkp (event_date → date_key)
  → JOIN utilisateur_lkp (utilisateur_id → utilisateur_key)
  → CALCUL age_on_event (F.months_between)
  → JOIN dim_age (age_on_event → age_key)
```

## 📊 Statistiques Données

### Comptages CSV Sources
```
user.csv:        10 lignes
session.csv:     10 lignes
product.csv:     10 lignes
product_name.csv: 5 lignes
events.csv:      20 lignes
brand.csv:        5 lignes
category.csv:     5 lignes
```

### Après Nettoyage
```
events_clean: 20 lignes
  - 0 suppressions timestamp NULL
  - 0 suppressions session_id NULL
  - 0 suppressions product_id NULL
  - 0 suppressions prix négatifs
  - 0 suppressions dates futures
```

### Après Star Schema
```
fact_events: 20 lignes (100% conservés)
  - Toutes dimensions liées
  - Aucune porte de qualité échouée
```

## ⚙️ Configuration Spark

```yaml
Spark Version: 4.0.1
Master: local[*]
Driver Memory: 8g
Shuffle Partitions: 200
Adaptive Query Execution: Enabled
Compression: snappy
```

## 🎓 Concepts Maîtrisés

✅ Star Schema design
✅ Dimension vs Fact tables
✅ Surrogate keys (dense_rank)
✅ Data quality gates
✅ Parquet vs CSV
✅ Window functions (OVER, PARTITION BY)
✅ Multi-table joins (LEFT joins)
✅ Date handling
✅ Price outlier detection
✅ Generation classification

## 📝 Cellules Notebook

| # | Titre | Lignes |
|---|-------|--------|
| 0 | Imports & Setup | 12 |
| 1 | Spark Init | 17 |
| 2 | Load CSV | 25 |
| 3 | dim_utilisateur | 18 |
| 4 | dim_age | 18 |
| 5 | dim_marque | 11 |
| 6 | dim_categorie | 11 |
| 7 | dim_produit | 32 |
| 8 | dim_date | 26 |
| 9 | Résumé dimensions | 18 |
| 10 | Clean events | 23 |
| 11 | Analyse prix | 28 |
| 12 | Lookup tables | 16 |
| 13 | fact_events | 52 |
| 14 | Affichage fact_events | 13 |
| 15 | Portes qualité | 56 |
| 16 | Exports | 29 |
| 17 | Comparaison sizes | 35 |
| 18 | Spark plans | 12 |
| 19 | Résumé final | 40 |

**Total: 507 lignes de code PySpark**

## 🏆 Résultats Finaux

- ✅ Star schema complet et validé
- ✅ 6 dimensions de haute qualité
- ✅ 1 table de faits avec 20 événements
- ✅ 3 formats d'export (CSV, Snappy, Parquet)
- ✅ 100% portes de qualité validées
- ✅ Compression optimale (Parquet 2x plus petit)
- ✅ Aucun data quality issue
- ✅ Intégrité référentielle 100%

## 📚 Références

- Apache Spark Documentation: https://spark.apache.org/docs/latest/
- PySpark API: https://spark.apache.org/docs/latest/api/python/
- Star Schema: https://en.wikipedia.org/wiki/Star_schema

---

**Rapport généré le**: Décembre 8, 2025
**Auteur**: Badr TAJINI
**Institution**: ESIEE Paris
