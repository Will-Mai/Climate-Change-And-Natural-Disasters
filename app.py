import os
import streamlit as st
from climate_disasters_pipeline import (
    load_disaster_data,
    build_merged_dataset,
    compute_disaster_summary,
    disaster_type_counts,
)

st.title("ENG 220 – Climate Change & Natural Disasters")

# Base path for data (folder where app.py lives)
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Build main merged table
temps_annual, disasters_per_year, merged = build_merged_dataset(base_path=BASE_PATH)

# Load full disaster events for type counts
disasters_all, _ = load_disaster_data(base_path=BASE_PATH)

summary_stats = compute_disaster_summary(merged)
type_counts = disaster_type_counts(disasters_all)

st.subheader("Annual Temperature vs. Disaster Counts")
st.line_chart(merged.set_index("year")[["TempF", "disaster_count"]])

st.subheader("Summary Statistics (Disasters per Year)")
st.json(summary_stats)

st.subheader("Most Common Disaster Types")

# Let the viewer choose how many top types to display
top_n = st.slider(
    "Number of top disaster types to display",
    min_value=5,
    max_value=min(30, len(type_counts)),
    value=min(10, len(type_counts)),
)

# Take the top N most common types
type_counts_top = type_counts.head(top_n)

# Convert Series -> DataFrame so Streamlit uses disaster_type as x-axis
type_counts_df = type_counts_top.reset_index()
type_counts_df.columns = ["disaster_type", "count"]

st.bar_chart(type_counts_df.set_index("disaster_type"))


st.subheader("Histogram of Disaster Counts per Year")
st.bar_chart(merged.set_index("year")["disaster_count"])

