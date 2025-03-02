"""
CrocoLake - A datalake for ocean observations
"""

from .loader.data_loader import DataLoader
from .converters.csv_converter import CSVConverter
from .converters.netcdf_converter import NetCDFConverter

__version__ = "0.1.0"

__all__ = [
    "DataLoader",
    "CSVConverter",
    "NetCDFConverter"
] 