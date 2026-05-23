# 📋 Schémas de Données Détaillés

## Bronze Layer (Sources)

### lab2_customers.csv
```
customer_id: INT (PK)
name: STRING
email: STRING
created_at: TIMESTAMP
```
**Exemple:**
```
1,Alice Martin,customer1@email.com,2024-01-01 00:00:00
```

### lab2_brands.csv
```
brand_id: INT (PK)
brand_name: STRING
```
**Exemple:**
```
1,TechCorp
```

### lab2_categories.csv
```
category_id: INT (PK)
category_name: STRING
```
**Exemple:**
```
1,Smartphones
```

### lab2_products.csv
```
product_id: INT (PK)
product_name: STRING
brand_id: INT (FK)
category_id: INT (FK)
price: DOUBLE
```
**Exemple:**
```
1,iPhone 15,1,1,999.99
```

### lab2_orders.csv
```
order_id: INT (PK)
customer_id: INT (FK)
order_date: TIMESTAMP
```
**Exemple:**
```
1,1,2024-06-15 10:30:45
```

### lab2_order_items.csv
```
order_item_id: INT (PK)
order_id: INT (FK)
product_id: INT (FK)
quantity: INT
unit_price: DOUBLE
```
**Exemple:**
```
1,1,1,2,999.99
```

---

## Silver/Gold Layer (Dimensions)

### dim_customer
```
customer_sk: LONG (surrogate key, xxhash64)
customer_id: INT (natural key)
name: STRING
email: STRING
created_at: TIMESTAMP
```

### dim_brand
```
brand_sk: LONG
brand_id: INT
brand_name: STRING
```

### dim_category
```
category_sk: LONG
category_id: INT
category_name: STRING
```

### dim_product
```
product_sk: LONG (surrogate key)
product_id: INT (natural key)
product_name: STRING
brand_sk: LONG (FK → dim_brand)
category_sk: LONG (FK → dim_category)
price: DOUBLE
```

### dim_date
```
date_sk: LONG (YYYYMMDD as hash)
date: DATE
year: INT (partition)
month: INT (partition)
day: INT
dow: STRING (E, Mon-Sun)
quarter: INT (1-4)
week_of_year: INT
```

---

## Gold Layer (Fact Table)

### fact_sales
```
order_id: INT (business key)
date_sk: LONG (FK → dim_date)
customer_sk: LONG (FK → dim_customer)
product_sk: LONG (FK → dim_product)
quantity: INT (measure)
unit_price: DOUBLE (measure)
subtotal: DOUBLE (derived measure)
year: INT (partition)
month: INT (partition)
```

**Contraintes:**
- `order_id` NOT NULL
- `date_sk` NOT NULL (FK)
- `customer_sk` NOT NULL (FK)
- `product_sk` NOT NULL (FK)
- `quantity > 0`
- `subtotal = quantity × unit_price`

---

## Statistiques Actuelles

| Table | Rows | Columns | Size (approx) |
|-------|------|---------|---------------|
| customers | 10 | 4 | <1 KB |
| brands | 5 | 2 | <1 KB |
| categories | 5 | 2 | <1 KB |
| products | 20 | 5 | <1 KB |
| orders | 50 | 3 | <1 KB |
| order_items | 100 | 5 | <1 KB |
| **fact_sales** | **100** | **9** | **<1 KB** |

---

## Exemple de Données Enrichies

**Before (Bronze):**
```
order_item_id=1, order_id=1, product_id=1, quantity=2, unit_price=999.99
```

**After (Gold):**
```
order_id=1
date_sk=20240615 (from orders)
customer_sk=4521968735... (hash of customer_id=1)
product_sk=1234567890... (hash of product_id=1)
quantity=2
unit_price=999.99
subtotal=1999.98
year=2024
month=6
```

---

**Voir aussi:** ARCHITECTURE.md pour les diagrammes
