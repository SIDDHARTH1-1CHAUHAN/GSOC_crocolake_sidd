import pandas as pd
from typing import Optional, List, Dict, Any
from pathlib import Path

class DataLoader:
    """Unified interface for loading CrocoLake datasets."""
    
    def __init__(self, data_dir: Optional[str] = None):
        """
        Initialize the data loader.
        
        Args:
            data_dir: Optional root directory containing the datasets
        """
        self.data_dir = Path(data_dir) if data_dir else Path.cwd()
    
    def load_dataset(
        self,
        dataset_name: str,
        variables: Optional[List[str]] = None,
        time_range: Optional[tuple] = None,
        bbox: Optional[Dict[str, float]] = None,
        depth_range: Optional[tuple] = None
    ) -> pd.DataFrame:
        """
        Load a dataset with optional filtering.
        
        Args:
            dataset_name: Name of the dataset to load
            variables: Optional list of variables to load
            time_range: Optional tuple of (start_time, end_time)
            bbox: Optional dict with keys 'min_lat', 'max_lat', 'min_lon', 'max_lon'
            depth_range: Optional tuple of (min_depth, max_depth)
        
        Returns:
            DataFrame containing the requested data
        """
        # Construct path to dataset
        dataset_path = self.data_dir / f"{dataset_name}.parquet"
        
        if not dataset_path.exists():
            raise FileNotFoundError(f"Dataset {dataset_name} not found at {dataset_path}")
        
        # Read the parquet file
        df = pd.read_parquet(dataset_path)
        
        # Apply filters
        df = self._apply_filters(
            df,
            variables=variables,
            time_range=time_range,
            bbox=bbox,
            depth_range=depth_range
        )
        
        return df
    
    def list_datasets(self) -> List[str]:
        """List available datasets in the data directory."""
        return [
            p.stem for p in self.data_dir.glob("*.parquet")
        ]
    
    def get_dataset_info(self, dataset_name: str) -> Dict[str, Any]:
        """
        Get metadata about a dataset.
        
        Args:
            dataset_name: Name of the dataset
        
        Returns:
            Dictionary containing dataset metadata
        """
        dataset_path = self.data_dir / f"{dataset_name}.parquet"
        
        if not dataset_path.exists():
            raise FileNotFoundError(f"Dataset {dataset_name} not found")
        
        # Read a small sample to get metadata
        df = pd.read_parquet(dataset_path)
        
        return {
            'variables': df['variable'].unique().tolist(),
            'time_range': (df['timestamp'].min(), df['timestamp'].max()),
            'spatial_coverage': {
                'min_lat': df['latitude'].min(),
                'max_lat': df['latitude'].max(),
                'min_lon': df['longitude'].min(),
                'max_lon': df['longitude'].max(),
            },
            'depth_range': (df['depth'].min(), df['depth'].max()),
            'n_observations': len(df),
            'sources': df['source'].unique().tolist()
        }
    
    def _apply_filters(
        self,
        df: pd.DataFrame,
        variables: Optional[List[str]] = None,
        time_range: Optional[tuple] = None,
        bbox: Optional[Dict[str, float]] = None,
        depth_range: Optional[tuple] = None
    ) -> pd.DataFrame:
        """Apply filters to the DataFrame."""
        if variables:
            df = df[df['variable'].isin(variables)]
        
        if time_range:
            start_time, end_time = time_range
            df = df[
                (df['timestamp'] >= start_time) &
                (df['timestamp'] <= end_time)
            ]
        
        if bbox:
            df = df[
                (df['latitude'] >= bbox['min_lat']) &
                (df['latitude'] <= bbox['max_lat']) &
                (df['longitude'] >= bbox['min_lon']) &
                (df['longitude'] <= bbox['max_lon'])
            ]
        
        if depth_range:
            min_depth, max_depth = depth_range
            df = df[
                (df['depth'] >= min_depth) &
                (df['depth'] <= max_depth)
            ]
        
        return df 