"""
Monitoring de performance - Temps et mémoire
"""

import time
import os
import platform
import sys

try:
    import psutil
except ImportError:
    psutil = None

try:
    import resource
except ImportError:
    resource = None


class PerformanceMonitor:
    """Mesure temps d'exécution et mémoire"""
    
    def __init__(self, name="Operation"):
        self.name = name
        self.start_time = None
        self.start_rss = None
        self.start_peak = None
        
    def __enter__(self):
        self.start_time = time.perf_counter()
        self.start_rss = self._get_rss_bytes()
        self.start_peak = self._get_peak_bytes()
        return self
        
    def __exit__(self, *args):
        end_time = time.perf_counter()
        end_rss = self._get_rss_bytes()
        end_peak = self._get_peak_bytes()
        
        wall_time = end_time - self.start_time
        rss_delta_mb = (end_rss - self.start_rss) / (1024 * 1024)
        peak_delta_mb = (end_peak - self.start_peak) / (1024 * 1024)
        
        self._print_results(wall_time, rss_delta_mb, peak_delta_mb)
    
    @staticmethod
    def _get_rss_bytes():
        """Mémoire résident en bytes"""
        if psutil is not None:
            return psutil.Process(os.getpid()).memory_info().rss
        return 0
    
    @staticmethod
    def _get_peak_bytes():
        """Pic de mémoire en bytes"""
        sysname = platform.system()
        
        if sysname == "Windows" and psutil is not None:
            mi = psutil.Process(os.getpid()).memory_info()
            peak = getattr(mi, "peak_wset", None)
            if peak is not None:
                return int(peak)
            return int(mi.rss)
        
        if resource is not None:
            try:
                ru = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                if sysname == "Linux":
                    return int(ru) * 1024
                else:
                    return int(ru)
            except Exception:
                pass
        
        return PerformanceMonitor._get_rss_bytes()
    
    def _print_results(self, wall_time, rss_delta_mb, peak_delta_mb):
        print("\n" + "="*50)
        print(f"⏱️  {self.name}")
        print("="*50)
        print(f"⏱️  Temps mur: {wall_time:.3f} s")
        print(f"📊 Mémoire RSS Δ: {rss_delta_mb:+.2f} MB")
        print(f"📈 Pic mémoire Δ: {peak_delta_mb:+.2f} MB")
        print("="*50 + "\n")


def measure_time_memory(func):
    """Décorateur pour mesurer temps et mémoire"""
    def wrapper(*args, **kwargs):
        with PerformanceMonitor(func.__name__):
            return func(*args, **kwargs)
    return wrapper

