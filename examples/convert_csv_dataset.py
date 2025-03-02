"""
Example script demonstrating how to convert a CSV dataset to CrocoLake format.
"""

import os
from pathlib import Path
from crocolake.converters import CSVConverter

def main():
    # Get absolute paths
    base_dir = Path(__file__).parent.parent
    source_path = base_dir / "data/raw/ocean_temp_data.csv"
    target_path = base_dir / "data/processed/ocean_temp_data.parquet"
    
    # Example configuration
    config = {
        'source_path': str(source_path),
        'target_path': str(target_path),
        'mapping': {
            'time': 'timestamp',
            'latitude': 'latitude',
            'longitude': 'longitude',
            'depth_m': 'depth',
            'temperature_c': 'temp',
            'salinity_psu': 'sal'
        }
    }
    
    # Create output directory if it doesn't exist
    os.makedirs(target_path.parent, exist_ok=True)
    
    # Initialize and run converter
    converter = CSVConverter.from_config(config)
    
    try:
        converter.convert()
        print(f"Successfully converted {source_path} to {target_path}")
    except Exception as e:
        print(f"Error converting dataset: {str(e)}")

if __name__ == '__main__':
    main() 