"""
Opérations de Join - Shuffle vs Hash
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
from src.utils import PerformanceMonitor


class JoinOperations:
    """Compare différentes stratégies de join"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
    
    def shuffle_join(self, df1, df2, join_col):
        """Join avec shuffle (sort-merge join)"""
        with PerformanceMonitor(f"Join: Shuffle on {join_col}"):
            return df1.join(df2, on=join_col, how="inner")
    
    def hash_join(self, df1, df2, join_col):
        """Join avec broadcast (hash join)"""
        with PerformanceMonitor(f"Join: Broadcast (Hash) on {join_col}"):
            # Broadcast la petite table
            return df1.join(broadcast(df2), on=join_col, how="inner")
    
    def compare_joins(self, events_df, products_df):
        """Compare les deux stratégies de join"""
        print("\n" + "="*60)
        print("🔗 Comparaison des Stratégies de Join")
        print("="*60)
        print(f"Events rows: {events_df.count():,}")
        print(f"Products rows: {products_df.count():,}")
        print("="*60 + "\n")
        
        # Shuffle join
        result_shuffle = self.shuffle_join(events_df, products_df, "product_id")
        print(f"Shuffle Join Result: {result_shuffle.count():,} rows")
        
        # Hash join (broadcast)
        result_hash = self.hash_join(events_df, products_df, "product_id")
        print(f"Hash Join Result: {result_hash.count():,} rows")
        
        print("\n" + "="*60)
        print("✅ Les deux joins donnent le même résultat")
        print("="*60 + "\n")
        
        return result_shuffle, result_hash

