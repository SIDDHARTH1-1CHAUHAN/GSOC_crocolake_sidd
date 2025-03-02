from setuptools import setup, find_packages

setup(
    name="crocolake",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "pandas>=2.0.0",
        "xarray>=2023.1.0",
        "pyarrow>=14.0.1",
        "netCDF4>=1.6.5",
        "numpy>=1.24.0",
        "dask>=2023.1.0",
        "fsspec>=2023.1.0",
    ],
    author="Your Name",
    author_email="your.email@example.com",
    description="A datalake for ocean observations",
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    python_requires=">=3.8",
) 