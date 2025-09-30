import streamlit as st
import pandas as pd
import plotly.express as px
import json
import folium
from streamlit_folium import st_folium
import shapely.geometry
from sqlalchemy import create_engine
import psycopg2
import os
from supabase import create_client

# -----------------------------
# Load & clean data
# -----------------------------
@st.cache_data
def get_db_password():
    try:
        # Try to get password from Streamlit secrets
        password = st.secrets["supabase"]["password"]
        if not password:
            raise KeyError("Empty password in secrets")
        return password
    except Exception:
        # Fallback: read password from local file
        password_file = "./password"
        if os.path.exists(password_file):
            with open(password_file, "r") as f:
                return f.readline().strip()
        else:
            raise FileNotFoundError(
                "Database password not found in st.secrets or ./password file."
            )
def load_data():
    # -----------------------------
    # Supabase Postgres connection URI
    # -----------------------------=
    
    password = get_db_password()
    db_url = f"postgresql://postgres.rtewftvldajjhqjbwwfx:{password}@aws-1-us-east-2.pooler.supabase.com:5432/postgres"

    # Create SQLAlchemy engine
    engine = create_engine(db_url)

    # -----------------------------
    # Fetch table data
    # -----------------------------
    st.info("Fetching data from Supabase PostgreSQL...")

    query = "SELECT * FROM cpi_long_with_location"
    df = pd.read_sql(query, engine)
    st.success(f"Fetched {len(df)} rows successfully!")
    df.to_csv("cpi_data.csv", index=False)  # Save a local copy

    # -----------------------------
    # Data cleaning
    # -----------------------------
    df['REF_DATE'] = pd.to_datetime(df['REF_DATE'], errors='coerce')
    df['GEO'] = df['GEO'].ffill()
    df['UOM'] = df['UOM'].ffill()
    df = df[['REF_DATE', 'GEO', 'UOM', 'Products and product groups', 'VALUE', 'MoM', 'YoY', 'City', 'Province']]
    df['VALUE'] = pd.to_numeric(df['VALUE'], errors='coerce')
    df[['VALUE', 'MoM', 'YoY']] = df[['VALUE', 'MoM', 'YoY']].fillna(method='ffill').fillna(method='bfill')
    df['Province'] = df['Province'].fillna(df['GEO'])

    return df

df = load_data()
st.set_page_config(page_title="CPI Dashboard", layout="wide")

# -----------------------------
# Sidebar filters
# -----------------------------
st.sidebar.title("Filters")

# View mode
view_mode = st.sidebar.radio("View mode", ["Line Graph", "Map + Line Graph"])

# Metric selection
metrics = ['VALUE', 'MoM', 'YoY']
selected_metric = st.sidebar.selectbox("Select metric", metrics)

# Categories selection
main_categories = [
    "All-items", "Food", "Shelter", "Household operations, furnishings and equipment",
    "Clothing and footwear", "Transportation", "Health and personal care",
    "Recreation, education and reading", "Alcoholic beverages, tobacco products and recreational cannabis"
]
all_categories = df['Products and product groups'].unique().tolist()
city_categories = ['All-items', 'Shelter']
import pandas as pd

# Year range
min_year = df['REF_DATE'].dt.year.min()
max_year = df['REF_DATE'].dt.year.max()
months = list(range(1, 13))  # 1=Jan, 12=Dec

# Use columns to put start year/month on the same line
st.sidebar.markdown("**Start Date**")
col1, col2 = st.sidebar.columns(2)
start_year = col1.number_input(f"Year (min = {min_year})", min_value=min_year, max_value=max_year, value=min_year, key="start_year")
start_month = col2.selectbox("Month", options=months, index=0, key="start_month")

st.sidebar.markdown("**End Date**")
col3, col4 = st.sidebar.columns(2)
end_year = col3.number_input(f"Year (max = {max_year})", min_value=min_year, max_value=max_year, value=max_year, key="end_year")
end_month = col4.selectbox("Month", options=months, index=0, key="end_month")

# Construct datetime objects at the first day of the month
start_date = pd.Timestamp(year=start_year, month=start_month, day=1)
end_date = pd.Timestamp(year=end_year, month=end_month, day=1)



# Plot height
height = st.sidebar.slider("Plot height", 500, 3000, 1000)

# -----------------------------
# Optional Line Graph filters
# -----------------------------
if view_mode == "Line Graph":
    level = st.sidebar.radio("Compare by", ["Province", "City"])
    if level == "Province":
        options = df['Province'].dropna().unique()
        default_selection = ['Ontario', 'British Columbia']
        selected_categories = st.sidebar.multiselect(
        "Select product categories", options=all_categories, default=main_categories
        )
        selected_areas = st.sidebar.multiselect("Select provinces", options, default=default_selection)
        filtered_df = df[df['Province'].isin(selected_areas)]
    else:
        options = df['City'].dropna().unique()
        default_selection = ['Toronto', 'Vancouver']
        selected_categories = st.sidebar.multiselect(
        "Select product categories", options=all_categories, default=city_categories
        )
        selected_areas = st.sidebar.multiselect("Select cities", options, default=default_selection)
        filtered_df = df[df['City'].isin(selected_areas)]
    compare_mode = st.sidebar.radio("Comparison mode", ['Cities', 'Categories'])
else:
    # Sidebar: number of categories per graph
    categories_per_graph = st.sidebar.slider(
    "Number of categories per graph", min_value=1, max_value=10, value=4, step=1
    )
    selected_categories = st.sidebar.multiselect(
    "Select product categories", options=all_categories, default=main_categories
    )
    filtered_df = df.copy()
    level = None
    selected_areas = None
    compare_mode = 'Categories'

# -----------------------------
# Filter data function
# -----------------------------
def filter_data(df_input):
    df = df_input.copy()
    cats = list(map(str, selected_categories))

    df = df[
        (df['Products and product groups'].isin(cats)) & 
        (df['REF_DATE'] >= pd.to_datetime(start_date)) &
        (df['REF_DATE'] <= pd.to_datetime(end_date)) &
        (df['UOM'] == '2002=100')
    ]
    return df



# Apply filtering
if view_mode == "Line Graph":
    filtered = filter_data(filtered_df)
    color_column = 'Province' if level == "Province" else 'City'
    filtered_sampled = filtered.groupby(
        ['REF_DATE', color_column, 'Products and product groups'], as_index=False
    )[selected_metric].mean()
else:
    filtered_line = filter_data(filtered_df)
    
    
# -----------------------------
# Plotting
# -----------------------------
st.title("CPI / Living Expenses Dashboard")

if view_mode == "Line Graph":
    color_column = 'Province' if level == "Province" else 'City'

    if compare_mode == 'Cities':
        # Each category in its own graph
        for category in filtered_sampled['Products and product groups'].unique():
            df_cat = filtered_sampled[filtered_sampled['Products and product groups'] == category]

            fig = px.line(
                df_cat,
                x='REF_DATE',
                y=selected_metric,
                color=color_column,
                markers=True,
                labels={
                    'REF_DATE': 'Date',
                    selected_metric: selected_metric,
                    color_column: level
                },
                title=f"{category} ({selected_metric})",
                height=height
            )
            fig.update_traces(marker=dict(size=4, opacity=0.7), line=dict(width=2))
            st.plotly_chart(fig, use_container_width=True)

    else:  # Province comparison or categories comparison
        for category in filtered_sampled['Products and product groups'].unique():
            df_cat = filtered_sampled[filtered_sampled['Products and product groups'] == category]

            fig = px.line(
                df_cat,
                x='REF_DATE',
                y=selected_metric,
                color=color_column,
                markers=True,
                labels={
                    'REF_DATE': 'Date',
                    selected_metric: selected_metric,
                    color_column: level
                },
                title=f"{category} ({selected_metric})",
                height=height
            )
            fig.update_traces(marker=dict(size=4, opacity=0.7), line=dict(width=2))
            st.plotly_chart(fig, use_container_width=True)


# -----------------------------
# Map + Line Graph
# -----------------------------
if view_mode == "Map + Line Graph":
    with open("canada_provinces.geojson") as f:
        geojson = json.load(f)

    m = folium.Map(location=[56, -96], zoom_start=3)
    folium.GeoJson(
        geojson,
        name="Provinces",
        style_function=lambda f: {'fillColor': '#ffffff', 'color': 'black', 'weight': 1, 'fillOpacity': 0.1},
        highlight_function=lambda f: {'fillColor': '#ffff00', 'color': 'black', 'weight': 2, 'fillOpacity': 0.5},
        tooltip=folium.GeoJsonTooltip(fields=['name'], aliases=['Province:'])
    ).add_to(m)

    st.subheader("Click a province to filter line graph")
    map_data = st_folium(m, width=800, height=500)

    clicked_province = None
    if map_data and map_data.get('last_clicked'):
        lat, lon = map_data['last_clicked']['lat'], map_data['last_clicked']['lng']
        point = shapely.geometry.Point(lon, lat)
        for feat in geojson['features']:
            polygon = shapely.geometry.shape(feat['geometry'])
            if polygon.contains(point):
                clicked_province = feat['properties']['name']
                break

    if clicked_province:
        # Keep only the clicked province
        filtered_line = filtered_line[
            filtered_line['Province'] == clicked_province
        ]
        
        # Sort for proper line plotting
        filtered_line = filtered_line.sort_values(
            ['Products and product groups', 'REF_DATE']
        )
        

        # Sort by product and date for proper chronological lines
        filtered_line['REF_DATE'] = pd.to_datetime(filtered_line['REF_DATE'], errors='coerce')
        filtered_line = filtered_line.sort_values(['Products and product groups', 'REF_DATE'])
        filtered_line = (
            filtered_line
            .groupby(['Products and product groups', 'REF_DATE'], as_index=False)[selected_metric]
            .mean()   # or sum(), depending on what makes sense
        )
        categories = filtered_line['Products and product groups'].unique().tolist()
        # Split categories into chunks of 4
        for i in range(0, len(categories), categories_per_graph):
            chunk = categories[i:i+categories_per_graph]
            df_chunk = filtered_line[filtered_line['Products and product groups'].isin(chunk)]

            fig_map = px.line(
                df_chunk,
                x='REF_DATE',
                y=selected_metric,
                color='Products and product groups',
                markers=True,
                labels={
                    'REF_DATE': 'Date',
                    selected_metric: selected_metric,
                    'Products and product groups': 'Category'
                },
                title=f"Categories {i+1} to {i+len(chunk)} - {clicked_province}",
                height=height
            )
            fig_map.update_traces(marker=dict(size=4, opacity=0.7), line=dict(width=2))
            st.plotly_chart(fig_map, use_container_width=True)

# -----------------------------
# Show raw filtered data
# -----------------------------
if st.checkbox("Show raw filtered data"):
    st.dataframe(filtered_df)
