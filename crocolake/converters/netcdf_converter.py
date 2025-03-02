import pandas as pd
import xarray as xr
from typing import Dict, Any, List, Optional
from .base import BaseConverter

class NetCDFConverter(BaseConverter):
    """Converter for NetCDF format data files."""
    
    def __init__(self, source_path: str, target_path: str = None,
                 variable_mapping: Dict[str, str] = None,
                 dimension_mapping: Dict[str, str] = None,
                 chunks: Optional[Dict[str, int]] = None):
        """
        Initialize the NetCDF converter.
        
        Args:
            source_path: Path to the source NetCDF file
            target_path: Optional path where to save the converted data
            variable_mapping: Dictionary mapping source variables to CrocoLake variables
            dimension_mapping: Dictionary mapping source dimensions to CrocoLake dimensions
            chunks: Dictionary specifying chunk sizes for dask arrays
        """
        super().__init__(source_path, target_path)
        self.variable_mapping = variable_mapping or {}
        self.dimension_mapping = dimension_mapping or {
            'time': 'timestamp',
            'lat': 'latitude',
            'lon': 'longitude',
            'depth': 'depth'
        }
        self.chunks = chunks
    
    def read_data(self) -> pd.DataFrame:
        """Read the NetCDF file into a pandas DataFrame."""
        # Open the dataset with optional chunking for large files
        ds = xr.open_dataset(self.source_path, chunks=self.chunks)
        
        # Rename dimensions according to mapping
        ds = ds.rename(self.dimension_mapping)
        
        # Convert to dataframe
        df = ds.to_dataframe().reset_index()
        
        # Close the dataset
        ds.close()
        
        return df
    
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the NetCDF data into CrocoLake's schema.
        
        This method handles the variable mapping and any necessary
        data transformations to match CrocoLake's schema.
        """
        # Melt the dataframe to get variables in long format
        id_vars = ['timestamp', 'latitude', 'longitude', 'depth']
        id_vars = [col for col in id_vars if col in data.columns]
        
        value_vars = [col for col in data.columns if col not in id_vars]
        
        df_long = data.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='variable',
            value_name='value'
        )
        
        # Map variable names if mapping is provided
        if self.variable_mapping:
            df_long['variable'] = df_long['variable'].map(
                self.variable_mapping).fillna(df_long['variable'])
        
        # Add source column
        df_long['source'] = self.source_path
        
        # Add unit column (should be extracted from NetCDF attributes in production)
        df_long['unit'] = 'unknown'
        
        return df_long
    
    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'NetCDFConverter':
        """
        Create a converter instance from a configuration dictionary.
        
        Args:
            config: Dictionary containing configuration parameters
                   Must include 'source_path' and optionally 'target_path',
                   'variable_mapping', 'dimension_mapping', and 'chunks'
        
        Returns:
            Configured NetCDFConverter instance
        """
        source_path = config.pop('source_path')
        target_path = config.pop('target_path', None)
        variable_mapping = config.pop('variable_mapping', None)
        dimension_mapping = config.pop('dimension_mapping', None)
        chunks = config.pop('chunks', None)
        
        return cls(
            source_path=source_path,
            target_path=target_path,
            variable_mapping=variable_mapping,
            dimension_mapping=dimension_mapping,
            chunks=chunks
        ) 