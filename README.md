# CrocoLake

CrocoLake is a datalake project for gathering physical and biogeochemical ocean observations, providing an efficient format and unified interface for data assimilation and ocean modeling activities.

## Project Overview

CrocoLakeTools contains Python modules to:
1. Convert existing datasets to CrocoLake's structure (parquet format with common schema)
2. Provide a unified interface to load datasets into a common dataframe

## Features

- Unified data schema for ocean observations
- Conversion tools for various data formats (CSV, NetCDF)
- Efficient parquet storage format
- Common interface for data loading and manipulation

## Installation

```bash
pip install -r requirements.txt
```

## Usage

```python
from crocolake.loader import DataLoader
from crocolake.converters import CSVConverter, NetCDFConverter

# Load data using the unified interface
loader = DataLoader()
data = loader.load_dataset("dataset_name")

# Convert a new dataset
converter = CSVConverter("path/to/data.csv")
converter.convert()
```

## Project Structure

```
crocolake/
├── __init__.py
├── converters/
│   ├── __init__.py
│   ├── base.py
│   ├── csv_converter.py
│   └── netcdf_converter.py
├── loader/
│   ├── __init__.py
│   └── data_loader.py
└── tests/
    ├── __init__.py
    └── test_converters.py
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details. 