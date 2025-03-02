import streamlit as st
import plotly.express as px
from pathlib import Path
from crocolake import DataLoader
import pandas as pd
from datetime import datetime, time

# Page config
st.set_page_config(
    page_title="CrocoLake Data Explorer",
    page_icon="🌊",
    layout="wide"
)

# Title
st.title("🌊 CrocoLake Data Explorer")
st.write("Explore ocean observations from various datasets")

# Initialize data loader
data_dir = Path("data/processed")
loader = DataLoader(str(data_dir))

# Sidebar
st.sidebar.header("Data Selection")

# Get available datasets
datasets = loader.list_datasets()
if not datasets:
    st.error("No datasets found in the processed directory!")
    st.stop()

# Dataset selector
selected_dataset = st.sidebar.selectbox(
    "Select Dataset",
    datasets
)

# Load dataset info
info = loader.get_dataset_info(selected_dataset)

# Variable selector
selected_variables = st.sidebar.multiselect(
    "Select Variables",
    info['variables'],
    default=[info['variables'][0]]
)

# Depth range slider
depth_min, depth_max = info['depth_range']
selected_depth = st.sidebar.slider(
    "Depth Range (m)",
    float(depth_min),
    float(depth_max),
    (float(depth_min), float(depth_max))
)

# Time range selector
time_min, time_max = info['time_range']
selected_dates = st.sidebar.date_input(
    "Time Range",
    (time_min.date(), time_max.date()),
    min_value=time_min.date(),
    max_value=time_max.date()
)

# Convert selected dates to datetime
if len(selected_dates) == 2:
    start_date, end_date = selected_dates
    selected_time = (
        datetime.combine(start_date, time.min),
        datetime.combine(end_date, time.max)
    )
else:
    selected_time = None

# Load filtered data
data = loader.load_dataset(
    selected_dataset,
    variables=selected_variables,
    depth_range=selected_depth,
    time_range=selected_time
)

# Display dataset info
st.header("Dataset Information")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Number of Observations", f"{len(data):,}")
with col2:
    st.metric("Variables", len(info['variables']))
with col3:
    st.metric("Depth Range", f"{depth_min:.1f}m to {depth_max:.1f}m")

# Create visualizations
st.header("Data Visualization")

# Time series plot
fig_time = px.scatter(
    data,
    x='timestamp',
    y='value',
    color='variable',
    title='Observations Over Time',
    labels={'value': 'Value', 'timestamp': 'Time'},
    hover_data=['depth', 'latitude', 'longitude']
)
st.plotly_chart(fig_time, use_container_width=True)

# Spatial distribution
fig_map = px.scatter_mapbox(
    data,
    lat='latitude',
    lon='longitude',
    color='value',
    size='value',
    hover_data=['depth', 'variable', 'timestamp'],
    title='Spatial Distribution',
    mapbox_style='carto-positron',
    zoom=8
)
st.plotly_chart(fig_map, use_container_width=True)

# Depth profile
fig_depth = px.scatter(
    data,
    x='value',
    y='depth',
    color='variable',
    title='Depth Profile',
    labels={'value': 'Value', 'depth': 'Depth (m)'},
    hover_data=['timestamp', 'latitude', 'longitude']
)
fig_depth.update_yaxes(autorange="reversed")  # Depth increases downward
st.plotly_chart(fig_depth, use_container_width=True)

# Raw data table
st.header("Raw Data")
st.dataframe(
    data.sort_values('timestamp'),
    hide_index=True,
    use_container_width=True
) 