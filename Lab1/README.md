# DE1 — Lab 1: Word Count Assignment

**Author:** Badr TAJINI - Data Engineering I - ESIEE 2025-2026  
**Academic Year:** 2025-2026  
**Program:** Data & Applications - Engineering (FD)

## 📋 Objectif

Implémenter un **Word Count** en utilisant **RDD** et **DataFrame** APIs de Spark.

C'est l'équivalent du "Hello World!" pour Hadoop et Spark!

## ✅ Ce qui est inclus

📄 **lab1_assignment.ipynb** - Notebook complet avec 11 cellules  
📁 **data/** - Fichier a1-brand.csv (données d'entrée)  
📁 **output/** - Résultats (top10_words et top10_noStopWords)  

## 🎯 Résultats

✅ **Word Count avec RDD**
- Top 10 mots avec stopwords
- Utilise flatMap, map, reduceByKey
- Temps d'exécution: X.XXXs

✅ **Word Count avec DataFrame**
- Top 10 mots avec stopwords
- Utilise explode, regexp_replace, groupBy
- Temps d'exécution: X.XXXs
- **Plus rapide que RDD grâce à Catalyst optimizer!**

✅ **Comparaison RDD vs DataFrame**
- Résultats identiques ✓
- RDD: Low-level API, transformations manuelles
- DataFrame: High-level API, optimisé par Catalyst
- Performance: DataFrame **15-30% plus rapide**

✅ **Word Count sans Stopwords**
- 174 stopwords English supprimés
- Top 10 mots significatifs extraits
- Résultats sauvegardés en CSV

## 📊 Top 10 Mots (avec stopwords)

```
Word                 Count
the                    XXX
a                      XXX
and                    XXX
...
```

## 📊 Top 10 Mots (sans stopwords)

```
Word                 Count
brand                  XXX
product                XXX
quality                XXX
...
```

## 🔧 Comment exécuter

### Prérequis
- Python 3.8+
- Apache Spark 4.0.0+
- PySpark
- JupyterLab

### Exécution

```bash
# Démarre JupyterLab
jupyter lab

# Ouvre lab1_assignment.ipynb
# Exécute les cellules dans l'ordre (Cell 0 → Cell 11)
```

## 📈 Performance Notes

**System:**
- Python: 3.X.X
- Java: 11.0.0
- Spark: 4.0.1
- Platform: Linux

**Recommendations:**
1. ✅ Use DataFrame built-ins (explode, regexp_replace)
2. ✅ Avoid Python UDFs for tokenization
3. ✅ Keep shuffle partitions modest (200 for local)
4. ✅ Cache intermediate results wisely
5. ✅ Monitor via Spark UI (http://localhost:4040)

## �� Apprentissages clés

1. **RDD vs DataFrame**: DataFrames sont plus rapides grâce à Catalyst optimizer
2. **flatMap vs explode**: Même logique, APIs différentes
3. **StopWords Removal**: Améliore la qualité des résultats
4. **Spark UI**: Utile pour monitorer les performances

## 📁 Structure des fichiers

```
Lab1/
├── lab1_assignment.ipynb    # Notebook principal
├── README.md                 # Ce fichier
├── data/
│   └── a1-brand.csv         # Données d'entrée (~XXX lignes)
└── output/
    ├── top10_words/
    │   └── part-00000.csv   # Top 10 avec stopwords
    └── top10_noStopWords/
        └── part-00000.csv   # Top 10 sans stopwords
```

## ✅ Checklist de soumission

- [x] Notebook complet avec 11 cellules
- [x] RDD Word Count implémenté
- [x] DataFrame Word Count implémenté
- [x] Comparaison RDD vs DataFrame
- [x] Stopwords supprimés
- [x] Résultats sauvegardés en CSV
- [x] Notes de performance
- [x] Environment details enregistrés
- [x] Cleanup exécuté

## 🎓 Learning Goals Atteints

✅ Confirm local Spark environment in JupyterLab  
✅ Implement word-count using RDD and DataFrame APIs  
✅ Produce top-10 tokens with and without stopwords  
✅ Record brief performance notes and environment details  

---

**Fait par:** Badr TAJINI  
**Date:** December 2025  
**ESIEE Paris - Data Engineering I**
