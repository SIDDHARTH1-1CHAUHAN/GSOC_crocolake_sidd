"""
Example script demonstrating how to load and inspect a converted dataset.
"""

from pathlib import Path
from crocolake import DataLoader

def main():
    # Initialize loader with the processed data directory
    base_dir = Path(__file__).parent.parent
    data_dir = base_dir / "data/processed"
    loader = DataLoader(str(data_dir))
    
    # List available datasets
    print("Available datasets:")
    print(loader.list_datasets())
    print()
    
    # Get dataset info
    dataset_name = "ocean_temp_data"
    print(f"Dataset info for {dataset_name}:")
    info = loader.get_dataset_info(dataset_name)
    print(f"Variables: {info['variables']}")
    print(f"Time range: {info['time_range']}")
    print(f"Spatial coverage: {info['spatial_coverage']}")
    print(f"Depth range: {info['depth_range']}")
    print(f"Number of observations: {info['n_observations']}")
    print()
    
    # Load data with filters
    print("Loading filtered data:")
    data = loader.load_dataset(
        dataset_name,
        variables=['temp'],
        depth_range=(0, 5)
    )
    print("\nFirst few rows of filtered data:")
    print(data.head())

if __name__ == '__main__':
    main() 