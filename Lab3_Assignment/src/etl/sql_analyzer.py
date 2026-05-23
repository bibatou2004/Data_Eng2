"""
Analyse SQL - Requêtes et opérations
"""

from pyspark.sql import SparkSession
from src.utils import PerformanceMonitor


class SQLAnalyzer:
    """Exécute et analyse les requêtes SQL"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        
    def query_top_brands_by_sales(self):
        """Top 10 marques par ventes totales"""
        query = """
        SELECT 
            b.brand_id,
            b.brand_name,
            COUNT(e.event_id) as total_events,
            SUM(e.sales_amount) as total_sales,
            AVG(e.sales_amount) as avg_sales,
            MIN(e.sales_amount) as min_sales,
            MAX(e.sales_amount) as max_sales
        FROM events e
        JOIN products p ON e.product_id = p.product_id
        JOIN brands b ON p.brand_id = b.brand_id
        GROUP BY b.brand_id, b.brand_name
        ORDER BY total_sales DESC
        LIMIT 10
        """
        
        with PerformanceMonitor("SQL: Top 10 Brands by Sales"):
            return self.spark.sql(query)
    
    def query_product_performance(self):
        """Performance des produits"""
        query = """
        SELECT 
            p.product_id,
            p.product_name,
            b.brand_name,
            COUNT(e.event_id) as total_events,
            SUM(e.sales_amount) as total_sales,
            AVG(e.sales_amount) as avg_sales
        FROM events e
        JOIN products p ON e.product_id = p.product_id
        JOIN brands b ON p.brand_id = b.brand_id
        GROUP BY p.product_id, p.product_name, b.brand_name
        ORDER BY total_sales DESC
        LIMIT 20
        """
        
        with PerformanceMonitor("SQL: Product Performance"):
            return self.spark.sql(query)
    
    def query_event_statistics(self):
        """Statistiques des événements"""
        query = """
        SELECT 
            DATE(event_timestamp) as event_date,
            COUNT(*) as event_count,
            SUM(sales_amount) as daily_sales,
            AVG(sales_amount) as avg_sale,
            MIN(sales_amount) as min_sale,
            MAX(sales_amount) as max_sale
        FROM events
        GROUP BY DATE(event_timestamp)
        ORDER BY event_date DESC
        LIMIT 30
        """
        
        with PerformanceMonitor("SQL: Event Statistics"):
            return self.spark.sql(query)

