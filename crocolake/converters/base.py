from abc import ABC, abstractmethod
import pandas as pd
from typing import Optional, Dict, Any

class BaseConverter(ABC):
    """Base class for all data converters in CrocoLake."""
    
    def __init__(self, source_path: str, target_path: Optional[str] = None):
        """
        Initialize the converter.
        
        Args:
            source_path: Path to the source data file
            target_path: Optional path where to save the converted data
        """
        self.source_path = source_path
        self.target_path = target_path or self._default_target_path()
        
    @abstractmethod
    def read_data(self) -> pd.DataFrame:
        """Read the source data into a pandas DataFrame."""
        pass
    
    @abstractmethod
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform the data into CrocoLake's schema."""
        pass
    
    def validate_schema(self, data: pd.DataFrame) -> bool:
        """
        Validate that the DataFrame follows CrocoLake's schema.
        
        Required columns:
        - timestamp: Timestamp of the observation
        - latitude: Latitude in decimal degrees
        - longitude: Longitude in decimal degrees
        - depth: Depth in meters
        - variable: Name of the measured variable
        - value: Value of the measurement
        - unit: Unit of measurement
        - source: Source of the data
        """
        required_columns = [
            'timestamp', 'latitude', 'longitude', 'depth',
            'variable', 'value', 'unit', 'source'
        ]
        
        return all(col in data.columns for col in required_columns)
    
    def save_data(self, data: pd.DataFrame) -> None:
        """Save the data in parquet format."""
        data.to_parquet(
            self.target_path,
            engine='pyarrow',
            compression='snappy',
            index=False
        )
    
    def convert(self) -> None:
        """Convert the data from source format to CrocoLake format."""
        data = self.read_data()
        transformed_data = self.transform_data(data)
        
        if not self.validate_schema(transformed_data):
            raise ValueError("Transformed data does not match CrocoLake schema")
        
        self.save_data(transformed_data)
    
    def _default_target_path(self) -> str:
        """Generate default target path if none is provided."""
        return self.source_path.rsplit('.', 1)[0] + '.parquet' 