"""
Opérations RDD - Moyennes et transformations
"""

from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from src.utils import PerformanceMonitor


class RDDOperations:
    """Opérations RDD personnalisées"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.sc = spark.sparkContext
    
    def mean_naive(self, rdd: RDD) -> float:
        """
        Calcul naïf de la moyenne
        Accumule TOUS les éléments en mémoire
        """
        with PerformanceMonitor("RDD: Naive Mean"):
            total = rdd.sum()
            count = rdd.count()
            return total / count if count > 0 else 0
    
    def mean_welford(self, rdd: RDD) -> tuple:
        """
        Algorithme de Welford pour la moyenne
        Accumule seulement (count, mean)
        """
        with PerformanceMonitor("RDD: Welford Mean"):
            def update(acc, value):
                count, mean = acc
                count += 1
                delta = value - mean
                mean += delta / count
                return (count, mean)
            
            count, mean = rdd.fold((0, 0.0), update)
            return (count, mean)
    
    def mean_map_reduce(self, rdd: RDD) -> float:
        """
        Moyenne avec map-reduce
        Calcule (sum, count) par partition puis combine
        """
        with PerformanceMonitor("RDD: MapReduce Mean"):
            # Map: chaque élément → (sum, count)
            mapped = rdd.map(lambda x: (x, 1))
            
            # Reduce: combine (sum, count)
            sum_count = mapped.reduce(
                lambda acc, val: (acc[0] + val[0], acc[1] + val[1])
            )
            
            total_sum, total_count = sum_count
            return total_sum / total_count if total_count > 0 else 0

