# Lab 1 Assignment : Pipeline Streaming - Documentation Complète

## 📋 Vue d'ensemble

Ce lab implémente un **pipeline de streaming en temps réel** avec Apache Spark. L'objectif est de traiter des événements en continu, les agréger par fenêtres de temps, et démontrer les concepts de **watermarking** et **checkpointing** pour garantir la fiabilité du traitement.

**Environnement** :
- Python 3.10
- Apache Spark 4.0.0
- JupyterLab
- Parquet (format de sortie)

---

## 🎯 Objectifs

✅ Construire un pipeline Structured Streaming complet  
✅ Appliquer des agrégations avec fenêtres temporelles (windowed aggregations)  
✅ Implémenter un watermark pour gérer les données tardives  
✅ Assurer la livraison exactement une fois (exactly-once) via checkpointing  
✅ Mesurer les performances avant et après optimisation  
✅ Capturer les métriques et plans d'exécution  

---

## 🏗️ Architecture du Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    PIPELINE DE STREAMING                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                   │
│  SOURCE                TRANSFORMATIONS              SINK          │
│  ├─ JSON Files    →    ├─ Watermark          →  Parquet Files  │
│  │ (events)            ├─ Window (10min)                         │
│  │                     ├─ GroupBy                                │
│  │                     └─ Aggregation                            │
│  │                        (count, avg, min, max)                 │
│  │                                                               │
│  └─ Checkpointing: Garantit la fiabilité (exactly-once)        │
│                                                                   │
└─────────────────────────────────────────────────────────────────┘
```

---

## 📝 Structure des Données

### Schéma des Événements

```json
{
  "event_id": "evt_001",
  "user_id": "user_123",
  "event_time": "2025-05-17T10:05:30Z",
  "event_type": "purchase",
  "value": 156.5
}
```

| Colonne | Type | Description |
|---------|------|-------------|
| `event_id` | String | Identifiant unique de l'événement |
| `user_id` | String | Identifiant de l'utilisateur |
| `event_time` | Timestamp | Timestamp de l'événement |
| `event_type` | String | Type d'événement (purchase, click, etc.) |
| `value` | Double | Valeur numérique associée |

---

## 🔑 Concepts Clés

### 1️⃣ **Watermark (Filigrane)**

Permet de gérer les données qui arrivent en retard :
- Les données reçues **après** le watermark sont ignorées
- Protège contre les données perdues dans le réseau
- Réduit l'utilisation mémoire

```python
.withWatermark("event_time", "5 minutes")
```

**Exemple** :
```
Event Time: 10:05:00 + Watermark: 5 min = Accept until 10:10:00
Events arriving after 10:10:00 are dropped
```

### 2️⃣ **Windowing (Fenêtrage)**

Regroupe les événements par intervalles de temps :

```python
F.window(EVENT_TIME_COL, WINDOW_DURATION)
```

**Timeline** :
```
[10:00, 10:10] → Agrégation 1
[10:10, 10:20] → Agrégation 2
[10:20, 10:30] → Agrégation 3
```

### 3️⃣ **Checkpointing**

Sauvegarde l'état du traitement :
- Permet la récupération après défaillance
- Garantit la livraison **exactement une fois** (exactly-once)
- Stocké dans `outputs/lab1/checkpoint/`

```python
.option("checkpointLocation", "outputs/lab1/checkpoint")
```

### 4️⃣ **Output Modes**

| Mode | Description | Usage |
|------|-------------|-------|
| **append** | Ajoute seulement les nouvelles lignes | Windowed agg + Parquet ✅ |
| **update** | Met à jour les lignes modifiées | Streaming SQL |
| **complete** | Réécrit toutes les données | Petit volume |

---

## 📊 Déroulement du Pipeline

### **Partie 0 : Setup**
- Création de la session Spark
- Import des bibliothèques
- Création des répertoires

### **Partie 1 : Source de Streaming**
- Définition du schéma des événements
- Configuration de la source (lecture JSON)
- Paramètres : window duration, watermark delay

### **Partie 2 : Transformations**
- Application du watermark
- Windowed aggregation par `event_type`
- Calcul : count, avg, min, max

### **Partie 3 : Écriture en Parquet**
- Lancement de la requête
- Démarrage des micro-batches (trigger: 5s)
- Checkpoint activé

### **Partie 4 : Monitoring**
- Capture du plan d'exécution
- Collection des métriques
- Visualisation Spark UI

### **Partie 5 : Optimisation**
- Configuration modifiée (window: 5min, watermark: 2min)
- Nouvelle requête
- Ré-mesure des performances

### **Partie 6 : Métriques**
- Export CSV comparatif
- Analyse des gains

### **Partie 7 : Cleanup**
- Arrêt des streams
- Fermeture de la session

---

## 📈 Résultats Attendus

### Configuration Baseline
- **Window Duration** : 10 minutes
- **Watermark Delay** : 5 minutes
- **Trigger Interval** : 5 secondes

### Configuration Optimisée
- **Window Duration** : 5 minutes (⬇️ latence)
- **Watermark Delay** : 2 minutes (⬇️ mémoire)
- **Trigger Interval** : 3 secondes (⬆️ réactivité)

### Gains Attendus
- ✅ Latence réduite de ~30-40%
- ✅ Mémoire utilisée diminuée
- ✅ Throughput maintenu
- ✅ CPU plus efficace

---

## 📁 Fichiers Générés

```
outputs/lab1/
├── stream_sink/              # Données Parquet (baseline)
│   ├── part-00000.parquet
│   ├── part-00001.parquet
│   └── _SUCCESS
├── stream_sink_optimized/    # Données Parquet (optimisée)
├── checkpoint/               # État du checkpoint
└── checkpoint_optimized/     # État du checkpoint

proof/
├── plan_streaming.txt        # Plan d'exécution détaillé
└── screenshots/              # Captures Spark UI

📊 lab1_metrics_log.csv      # Comparaison des métriques
```

---

## 🔍 Monitoring en Temps Réel

Pendant l'exécution, consultez :

```
http://localhost:4040/StreamingQuery/
```

**Métriques visibles** :
- Input Rate (événements/sec)
- Processing Rate (lignes/sec)
- Batch Duration (temps du batch)
- Watermark (valeur actuelle)

---

## 📋 Checklist de Validation

- [ ] Streaming query démarre sans erreur
- [ ] Au moins 5 micro-batches traités
- [ ] `outputs/lab1/stream_sink/` contient des fichiers Parquet
- [ ] `outputs/lab1/checkpoint/` est non-vide
- [ ] `proof/plan_streaming.txt` généré
- [ ] `lab1_metrics_log.csv` a 2+ lignes
- [ ] Screenshots Spark UI capturées

---

## 🚀 Comment Exécuter

1. **Préparer les données** :
   ```bash
   mkdir -p data/streaming_input
   # Ajouter des fichiers JSON
   ```

2. **Lancer le notebook** :
   ```bash
   jupyter lab lab1_assignement.ipynb
   ```

3. **Exécuter cellule par cellule** (Shift + Enter)

4. **Consulter les résultats** :
   ```bash
   cat lab1_metrics_log.csv
   ls -la outputs/lab1/stream_sink/
   ```

---

## 🎓 Concepts Maîtrisés

✅ Structured Streaming API  
✅ Windowed Aggregations  
✅ Watermark Management  
✅ Checkpointing & Exactly-Once  
✅ Output Modes  
✅ Performance Optimization  
✅ Metrics Capture  

---

## 📚 Ressources

- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Watermarking Documentation](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#handling-late-data-and-watermarking)
- [Output Modes](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#output-modes)

---

**Auteur** : Badr TAJINI  
**Course** : Data Engineering II - ESIEE 2025-2026  
**Date** : Mai 2026