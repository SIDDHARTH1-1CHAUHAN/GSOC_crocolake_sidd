import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import tempfile
import os

from crocolake.converters import CSVConverter, NetCDFConverter

@pytest.fixture
def sample_csv_data():
    """Create a temporary CSV file with sample data."""
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False, mode='w') as f:
        f.write("""time,lat,lon,depth,temperature,salinity
2023-01-01 00:00:00,45.5,-125.5,0,15.2,33.1
2023-01-01 00:00:00,45.5,-125.5,10,14.8,33.2
2023-01-01 00:00:00,45.6,-125.4,0,15.3,33.0
""")
        return f.name

@pytest.fixture
def csv_mapping():
    """Sample column mapping for CSV data."""
    return {
        'time': 'timestamp',
        'lat': 'latitude',
        'lon': 'longitude',
        'temperature': 'temp',
        'salinity': 'sal'
    }

def test_csv_converter(sample_csv_data, csv_mapping):
    """Test CSV converter functionality."""
    # Create a temporary output file
    with tempfile.NamedTemporaryFile(suffix='.parquet', delete=False) as f:
        output_path = f.name
    
    # Initialize and run converter
    converter = CSVConverter(
        source_path=sample_csv_data,
        target_path=output_path,
        mapping=csv_mapping
    )
    converter.convert()
    
    # Read and validate the output
    df = pd.read_parquet(output_path)
    
    assert set(df.columns) == {'timestamp', 'latitude', 'longitude', 'depth',
                              'variable', 'value', 'unit', 'source'}
    assert len(df) == 6  # 2 variables * 3 original rows
    assert set(df['variable'].unique()) == {'temp', 'sal'}
    
    # Clean up temporary files
    os.unlink(sample_csv_data)
    os.unlink(output_path)

def test_csv_converter_from_config(sample_csv_data, csv_mapping):
    """Test CSV converter creation from config."""
    config = {
        'source_path': sample_csv_data,
        'mapping': csv_mapping,
        'target_path': 'test_output.parquet'
    }
    
    converter = CSVConverter.from_config(config)
    assert converter.source_path == sample_csv_data
    assert converter.mapping == csv_mapping
    
    # Clean up
    os.unlink(sample_csv_data) 