# DE1 — Lab 0: Installation and Sanity Checks

**Author:** Badr TAJINI - Data Engineering I - ESIEE 2025-2026

## �� Objectif

Prouver votre configuration locale en utilisant les métriques et les plans d'exécution Spark.

## ✅ Ce qui est inclus

- `Lab0.ipynb` - Notebook complet avec 8 cellules
- `metrics_log_template_en.csv` - Métriques Spark UI
- `data/sample_sales.csv` - Données de test
- `proof/plan_formatted.txt` - Plan d'exécution sauvegardé

## 🚀 Comment exécuter

1. **Sélectionne le kernel `de1-env`**
2. **Exécute chaque cellule** en ordre (Cell 0 → Cell 7)
3. **Ouvre Spark UI** à http://localhost:4040 pendant Cell 5
4. **Note les métriques** dans le CSV

## 📊 Résultats attendus

| Fichier | Description |
|---------|-------------|
| `proof/plan_formatted.txt` | Plan d'exécution Spark |
| `metrics_log_template_en.csv` | Métriques observées |
| `data/sample_sales.csv` | Données d'entrée |

## ✅ Checklist

- [x] Cell 0: Vérification de l'environnement
- [x] Cell 1: Vérification de PySpark
- [x] Cell 2: Génération et lecture du CSV
- [x] Cell 3: Plan d'exécution
- [x] Cell 4: Sauvegarde des preuves
- [x] Cell 5: Métriques Spark UI
- [x] Cell 6: Nettoyage
- [x] Cell 7: Export CSV

---

**Date complétée:** 2025-12-07

