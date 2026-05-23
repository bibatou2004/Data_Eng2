"""
Chargement des DataFrames Spark
"""

import os
from pyspark.sql import SparkSession
from src.config.settings import DATA_PATH, TABLES


class DataLoader:
    """Charge les DataFrames Spark"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.data_path = DATA_PATH
        
    def load_events(self):
        """Charge la table events"""
        path = os.path.join(self.data_path, TABLES["events"])
        return self.spark.read.parquet(path)
    
    def load_products(self):
        """Charge la table products"""
        path = os.path.join(self.data_path, TABLES["products"])
        return self.spark.read.parquet(path)
    
    def load_brands(self):
        """Charge la table brands"""
        path = os.path.join(self.data_path, TABLES["brands"])
        return self.spark.read.parquet(path)
    
    def load_all(self):
        """Charge toutes les tables"""
        return {
            "events": self.load_events(),
            "products": self.load_products(),
            "brands": self.load_brands(),
        }
    
    @staticmethod
    def print_schema_info(df, name):
        """Affiche le schéma et le nombre de lignes"""
        print(f"\n{'='*60}")
        print(f"📊 Table: {name}")
        print(f"{'='*60}")
        print(f"Rows: {df.count():,}")
        print(f"\nSchema:")
        df.printSchema()
        print(f"{'='*60}\n")

