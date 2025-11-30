import os

import streamlit as st
from climate_disasters_pipeline import (
    load_disaster_data,
    build_merged_dataset,
    compute_disaster_summary,
    disaster_type_counts,
)

st.title("ENG 220 – Climate Change & Natural Disasters")

# Base path is the folder where app.py lives (repo root on Streamlit Cloud)
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Build annual disaster table
disasters_per_year, merged = build_merged_dataset(base_path=BASE_PATH)

# Full events table for type counts
disasters_all, _ = load_disaster_data(base_path=BASE_PATH)

# Summary + type counts
summary_stats = compute_disaster_summary(merged)
type_counts = disaster_type_counts(disasters_all)

# ---- Charts ----------------------------------------------------------

st.subheader("Disaster Counts per Year")
st.line_chart(merged.set_index("year")[["disaster_count"]])

st.subheader("Summary Statistics (Disasters per Year)")
st.json(summary_stats)

st.subheader("Most Common Disaster Types (Top 16)")
type_counts_df = type_counts.reset_index()
type_counts_df.columns = ["disaster_type", "count"]

# Always show the 16 most common detailed types (Var5)
top_types = type_counts_df.head(16)
st.bar_chart(top_types.set_index("disaster_type"))

st.subheader("Histogram of Disaster Counts per Year")
st.bar_chart(merged.set_index("year")["disaster_count"])


