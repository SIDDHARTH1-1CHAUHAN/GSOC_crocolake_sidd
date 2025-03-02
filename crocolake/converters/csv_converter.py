import pandas as pd
from typing import Dict, Any
from .base import BaseConverter

class CSVConverter(BaseConverter):
    """Converter for CSV format data files."""
    
    def __init__(self, source_path: str, target_path: str = None, 
                 mapping: Dict[str, str] = None, **csv_kwargs: Any):
        """
        Initialize the CSV converter.
        
        Args:
            source_path: Path to the source CSV file
            target_path: Optional path where to save the converted data
            mapping: Dictionary mapping source columns to CrocoLake schema columns
            csv_kwargs: Additional keyword arguments passed to pd.read_csv
        """
        super().__init__(source_path, target_path)
        self.mapping = mapping or {}
        self.csv_kwargs = csv_kwargs
    
    def read_data(self) -> pd.DataFrame:
        """Read the CSV file into a pandas DataFrame."""
        return pd.read_csv(self.source_path, **self.csv_kwargs)
    
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Transform the CSV data into CrocoLake's schema.
        
        This method handles the column mapping and any necessary
        data transformations to match CrocoLake's schema.
        """
        # Rename columns according to mapping
        if self.mapping:
            data = data.rename(columns=self.mapping)
        
        # Ensure timestamp is in datetime format
        if 'timestamp' in data.columns:
            data['timestamp'] = pd.to_datetime(data['timestamp'])
        
        # Convert numeric columns
        numeric_columns = ['latitude', 'longitude', 'depth', 'value']
        for col in numeric_columns:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')
        
        # Get measurement columns (those not in the standard schema)
        id_vars = ['timestamp', 'latitude', 'longitude', 'depth']
        id_vars = [col for col in id_vars if col in data.columns]
        value_vars = [col for col in data.columns 
                     if col not in id_vars and col not in ['variable', 'value', 'unit', 'source']]
        
        # Melt the dataframe to get variables in long format
        df_long = data.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name='variable',
            value_name='value'
        )
        
        # Add source column
        df_long['source'] = self.source_path
        
        # Add unit column based on variable name
        # In a real application, this should be more sophisticated
        unit_mapping = {
            'temp': '°C',
            'sal': 'PSU'
        }
        df_long['unit'] = df_long['variable'].map(unit_mapping).fillna('unknown')
        
        return df_long

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'CSVConverter':
        """
        Create a converter instance from a configuration dictionary.
        
        Args:
            config: Dictionary containing configuration parameters
                   Must include 'source_path' and optionally 'target_path',
                   'mapping', and any CSV reading parameters
        
        Returns:
            Configured CSVConverter instance
        """
        source_path = config.pop('source_path')
        target_path = config.pop('target_path', None)
        mapping = config.pop('mapping', None)
        
        return cls(
            source_path=source_path,
            target_path=target_path,
            mapping=mapping,
            **config
        ) 