import os
import streamlit as st
from climate_disasters_pipeline import (
    load_disaster_data,
    build_merged_dataset,
    compute_disaster_summary,
    disaster_type_counts,
)

st.title("ENG 220 – Climate Change & Natural Disasters")

# Folder where app.py lives
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# Build main per-year table (disasters only)
disasters_per_year, merged = build_merged_dataset(base_path=BASE_PATH)

# Load full disaster events for type counts
disasters_all, _ = load_disaster_data(base_path=BASE_PATH)

# Summary statistics
summary_stats = compute_disaster_summary(merged)
type_counts = disaster_type_counts(disasters_all)

# ---- Plots ----

st.subheader("Disaster Counts per Year")
st.line_chart(merged.set_index("year")[["disaster_count"]])

st.subheader("Summary Statistics (Disasters per Year)")
st.json(summary_stats)

st.subheader("Most Common Disaster Types")
type_counts_df = type_counts.reset_index()
type_counts_df.columns = ["disaster_type", "count"]
top_types = type_counts_df.head(16)  # always show top 16
st.bar_chart(top_types.set_index("disaster_type"))

st.subheader("Histogram of Disaster Counts per Year")
st.bar_chart(merged.set_index("year")["disaster_count"])
