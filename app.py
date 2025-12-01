import os
import streamlit as st

from climate_disasters_pipeline import (
    build_merged_dataset,
    compute_disaster_summary,
    disaster_type_counts,
)

# Page config (optional but nice)
st.set_page_config(
    page_title="ENG 220 – Climate Change & Natural Disasters",
    layout="wide",
)

st.title("ENG 220 – Climate Change & Natural Disasters")

# --------------------------------------------------------------------
# Figure out project root so it works both locally and on Streamlit Cloud
# --------------------------------------------------------------------
BASE_PATH = os.path.dirname(os.path.abspath(__file__))

# If the app lives in "Data Cleaning Code/app.py", go up one level
if os.path.basename(BASE_PATH).lower() == "data cleaning code":
    BASE_PATH = os.path.dirname(BASE_PATH)

# --------------------------------------------------------------------
# Load data
# --------------------------------------------------------------------
disasters_per_year, merged, disasters_all = build_merged_dataset(BASE_PATH)

# Safety filter on year range
merged = merged[(merged["year"] >= 1970) & (merged["year"] <= 2022)]

# Compute stats
summary_stats = compute_disaster_summary(merged)
type_counts = disaster_type_counts(disasters_all)

# --------------------------------------------------------------------
# DISASTER COUNTS PER YEAR
# --------------------------------------------------------------------
st.subheader("Disaster Counts per Year (1970–2022)")
st.line_chart(merged.set_index("year")[["disaster_count"]])

# --------------------------------------------------------------------
# SUMMARY STATISTICS
# --------------------------------------------------------------------
st.subheader("Summary Statistics (Disasters per Year)")
st.json(summary_stats)

# --------------------------------------------------------------------
# MOST COMMON DISASTER TYPES
# --------------------------------------------------------------------
st.subheader("Most Common Disaster Types")

type_counts_df = type_counts.reset_index()
type_counts_df.columns = ["disaster_type", "count"]

# Show ALL available types (sorted)
type_counts_df = type_counts_df.sort_values("count", ascending=False)

st.bar_chart(type_counts_df.set_index("disaster_type"))

# --------------------------------------------------------------------
# HISTOGRAM OF DISASTER COUNTS
# --------------------------------------------------------------------
st.subheader("Histogram of Disaster Counts per Year")
st.bar_chart(merged.set_index("year")["disaster_count"])

# --------------------------------------------------------------------
# (Optional) Temperature vs Disaster Counts scatter
# --------------------------------------------------------------------
st.subheader("Temperature vs. Disaster Counts")
st.scatter_chart(
    merged.set_index("TempF")[["disaster_count"]],
)


