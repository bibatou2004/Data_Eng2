# 📊 Données Lab 2

## 📥 Entrées (entrees/)

7 fichiers CSV sources:

| Fichier | Lignes | Colonnes | Utilité |
|---------|--------|----------|---------|
| **user.csv** | 10 | user_id, gender, birthdate | Dimension utilisateurs |
| **session.csv** | 10 | session_id, user_id | Bridge session → user |
| **product.csv** | 10 | product_id, brand, category, product_name | Produits |
| **product_name.csv** | 5 | category, product_name, description | Descriptions enrichies |
| **events.csv** | 20 | event_time, session_id, product_id, event_type, price | Événements e-commerce |
| **brand.csv** | 5 | brand, description | Marques |
| **category.csv** | 5 | category, description | Catégories |

**Total entrées:** ~2 KB

### Schéma user.csv
```
user_id,gender,birthdate
1,M,1985-05-15
2,F,1992-03-22
...
```

### Schéma events.csv
```
event_time,session_id,product_id,event_type,price
2024-12-01 10:30:45,S001,P001,view,
2024-12-01 10:35:22,S001,P001,cart,49.99
2024-12-01 10:40:15,S001,P001,purchase,49.99
...
```

---

## 📤 Sorties (sorties/)

3 formats de la table de faits `fact_events`:

### 1. CSV Brut (fact_events_csv/)
```
Compression: Aucune
Extension: .csv
Colonnes: date_key,user_key,age_key,product_key,brand_key,category_key,session_id,event_time,event_type,price
Lignes: 20
Taille: ~0.0010 MB
```

### 2. CSV Snappy (fact_events_csv_snappy/)
```
Compression: Snappy
Extension: .csv
Colonnes: Identiques au CSV brut
Lignes: 20
Taille: ~0.0008 MB
```

### 3. Parquet (fact_events_parquet/)
```
Compression: Snappy (par défaut)
Extension: .parquet
Colonnes: Identiques
Lignes: 20
Taille: ~0.0005 MB (2x plus petit!)
```

---

## 🔍 Schéma fact_events (Sortie)

| Colonne | Type | Description |
|---------|------|-------------|
| **date_key** | INT | FK → dim_date (YYYYMMDD) |
| **user_key** | INT | FK → dim_user |
| **age_key** | INT | FK → dim_age |
| **product_key** | INT | FK → dim_product |
| **brand_key** | INT | FK → dim_brand |
| **category_key** | INT | FK → dim_category |
| **session_id** | STRING | Clé métier (identifie la session) |
| **event_time** | TIMESTAMP | Quand l'événement s'est produit |
| **event_type** | STRING | Type: view, cart, purchase, remove |
| **price** | DOUBLE | Montant (NULL si event_type=view) |

### Exemple ligne fact_events:
```
20241201, 1, 5, 3, 2, 1, S001, 2024-12-01T10:40:15.000Z, purchase, 49.99
```

---

## 📊 Statistiques

### Comptages
- **Utilisateurs**: 10 (dim_user)
- **Groupes d'âge**: 10 (dim_age)
- **Marques**: 5 (dim_brand)
- **Catégories**: 5 (dim_categorie)
- **Produits**: 10 (dim_product)
- **Dates**: 4 (2024-12-01 à 2024-12-04)
- **Événements**: 20 (fact_events)

### Compression
| Format | Taille | Ratio |
|--------|--------|-------|
| CSV | 0.0010 MB | 2.0x |
| CSV Snappy | 0.0008 MB | 1.6x |
| **Parquet** | **0.0005 MB** | **1.0x** |

---

## 🔄 Reproducibilité

Pour régénérer ces sorties:

```bash
# Exécute le notebook
jupyter lab ../notebooks/lab2_assignment.ipynb

# Les sorties seront créées dans:
# ../donnees/sorties/
```

---

## 📝 Notes

- Toutes les données sont **fictives** (pour fins d'apprentissage)
- Les dates vont du 1er au 4 décembre 2024
- Les prix sont en euros (EUR)
- Aucune donnée personnelle réelle

---

**Généré par Lab 2 ETL Pipeline**
**Date**: Décembre 8, 2025
