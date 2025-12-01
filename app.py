import os
import streamlit as st
from climate_disasters_pipeline import (
    load_disaster_data,
    build_merged_dataset,
    compute_disaster_summary,
    disaster_type_counts,
)

st.title("ENG 220 – Climate Change & Natural Disasters")

BASE_PATH = os.path.dirname(os.path.abspath(__file__))

disasters_per_year, merged = build_merged_dataset(base_path=BASE_PATH)
disasters_all, _ = load_disaster_data(base_path=BASE_PATH)

summary_stats = compute_disaster_summary(merged)
type_counts = disaster_type_counts(disasters_all)

st.subheader("Disaster Counts per Year")
st.line_chart(merged.set_index("year")[["disaster_count"]])

st.subheader("Summary Statistics (Disasters per Year)")
st.json(summary_stats)

st.subheader("Most Common Disaster Types (Top 16)")
df = type_counts.reset_index()
df.columns = ["disaster_type", "count"]
st.bar_chart(df.head(16).set_index("disaster_type"))

st.subheader("Histogram of Disaster Counts per Year")
st.bar_chart(merged.set_index("year")["disaster_count"])



