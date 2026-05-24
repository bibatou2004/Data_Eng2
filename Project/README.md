# 🎮 DE2 Final Project - eSports Data Pipeline

## Abibatou WANDAOGO et Youcef GUEDIRI

Nous nous sommes aidés de l'IAGen pour construire ce projet et bien comprendre les notions.

## 📝 Présentation du Projet
Ce projet implémente un pipeline complet de Data Engineering traitant plus de **20 millions de lignes** de données eSports brutes. L'architecture repose sur **Apache Spark (PySpark)** et traite des problématiques de Batch (ETL), de Streaming, de NLP et de Machine Learning.

## 🏗️ Architecture du Pipeline

1. **ETL Batch (Bronze ➡️ Silver ➡️ Gold)**
   - **Bronze** : Ingestion immuable des données brutes (`match.csv`, `players.csv`) au format Parquet.
   - **Silver** : Nettoyage, typage, suppression des valeurs nulles et jointure optimisée (Broadcast) entre les matchs et les joueurs.
   - **Gold** : Agrégation des statistiques par joueur et par mois, avec partitionnement dynamique (`year`, `month`) sur le disque.

2. **Structured Streaming**
   - Simulation d'un flux d'achats en jeu (`purchase_log.csv`).
   - Agrégation par fenêtres temporelles (Windows) et gestion du retard avec un Watermark de 10 minutes.
   - Sauvegarde continue (Append) avec Checkpointing.

3. **Traitement Textuel (NLP)**
   - Nettoyage et tokenisation de 1,4 million de messages de chat en jeu.
   - Création d'un **Index Inversé** sauvegardé en Parquet.
   - *Performance* : Latence de requête de ~65ms pour la recherche de termes (SLO < 2s validé).

4. **Machine Learning (Clustering Iteratif)**
   - Profilage des joueurs avec `KMeans` sur 3 features : kills, deaths, gold_per_min.
   - Évaluation de plusieurs clusters (K=3, K=5, K=7). Le meilleur score de Silhouette a été trouvé pour **K=3 (0.4523)**.
   - *Optimisation* : Le repartitionnement ciblé des données (8 partitions) a permis un gain de temps d'exécution de plus de 36%.

5. **LLM Data Readiness**
   - Filtrage qualitatif du chat et formatage de prompts prêts à être ingérés par un Large Language Model pour analyse de sentiment/toxicité.

## 📂 Structure du Dépôt

Le dossier a été configuré pour ignorer les gigaoctets de données locales via `.gitignore`. Voici ce qui est versionné :

```text
Project/
│
├── DE2_Project_Notebook_EN.ipynb   # Le code principal (Pipeline complet)
├── data_generator.py               # Script de vérification/génération du volume de données
├── de2_project_config.yml          # Configuration centralisée (Paths, SLOs, Params ML)
├── project_metrics_log.csv         # Registre automatique des temps d'exécution et métriques
├── README.md                       # Documentation
│
└── proof/                          # 📁 Preuves d'exécution pour l'évaluation
    ├── plan_etl.txt                # Plan physique d'exécution de la table Gold
    ├── plan_iterative.txt          # Plan d'exécution du Clustering KMeans
    ├── query_progress.json         # Preuve JSON de l'exécution du Structured Streaming
    └── *.png                       # Captures d'écran de la Spark UI (Jobs, SQL, Streaming)