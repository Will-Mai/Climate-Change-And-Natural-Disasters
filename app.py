import os
import streamlit as st
from climate_disasters_pipeline import (
    load_disaster_data,
    build_merged_dataset,
    compute_disaster_summary,
    disaster_type_counts,
)

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

disasters_per_year, merged = build_merged_dataset(base_path=BASE_PATH)
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
top_types = type_counts_df.head(16)  # always show top 16 types
st.bar_chart(top_types.set_index("disaster_type"))

st.subheader("Histogram of Disaster Counts per 5-Year Period")

# Create 5-year bins (e.g., 1980, 1985, 1990, ...)
hist_df = merged.copy()
hist_df["year_bin"] = (hist_df["year"] // 5) * 5  # start year of each 5-year block

hist_counts = (
    hist_df.groupby("year_bin", as_index=False)["disaster_count"]
    .sum()
    .sort_values("year_bin")
)

st.bar_chart(hist_counts.set_index("year_bin")["disaster_count"])
