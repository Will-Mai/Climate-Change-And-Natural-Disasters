# Auto-converted from climate-disasters-pipeline.ipynb

"""
ENG 220 – Climate Change & Natural Disasters
Data Pipeline for Streamlit

This module provides functions to:
- Load and clean global temperature datasets
- Load and clean natural disaster datasets
- Aggregate to annual level
- Merge temperature + disaster counts
- Compute summary statistics and type frequencies

Files expected (in base_path or Kaggle input path):
- Gia_Bch_Nguyn_Earth_Temps_Cleaned.csv
- Berkeley_Earth_Temps_Cleaned.csv
- Josep_Ferrer_Temps_Cleaned.csv
- Baris_Dincer_Disasters_Cleaned.csv
- Shreyansh_Dangi_Disasters_Cleaned.csv
"""

# climate_disasters_pipeline.py
# Minimal disasters-only pipeline for ENG 220 Streamlit app

import os
import pandas as pd
from typing import Tuple, Dict


# ---------------------------------------------------------------------
# LOAD DISASTER DATA (Var5 = true disaster type)
# ---------------------------------------------------------------------
def load_disaster_data(base_path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load cleaned disaster dataset and compute counts per year."""

    dis_path = os.path.join(
        base_path, "Cleaned Data", "Natural Disasters", "Baris_Dincer_Disasters_Cleaned.csv"
    )

    df = pd.read_csv(dis_path)

    # Your dataset columns = EventDate, Var2, Var3, Var4, Var5
    df.columns = ["event_date", "region", "category", "subcategory", "disaster_type"]

    # Fix spacing / formatting
    df["disaster_type"] = df["disaster_type"].astype(str).str.strip()

    # Parse dates
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df = df.dropna(subset=["event_date"])

    df["year"] = df["event_date"].dt.year.astype(int)

    # Remove garbage future years
    df = df[(df["year"] >= 1970) & (df["year"] <= 2022)]

    # Count disasters per year
    disasters_per_year = df.groupby("year").size().reset_index(name="disaster_count")

    return df, disasters_per_year


# ---------------------------------------------------------------------
# LOAD TEMPERATURE DATA (monthly → annual averages)
# ---------------------------------------------------------------------
def load_temperature_data(base_path: str) -> pd.DataFrame:
    """
    Load the Berkeley Earth temperature data and aggregate to annual means.

    This version is defensive about column names so it won't crash if the CSV
    has an extra index column.
    """
    temps_path = os.path.join(
        base_path,
        "Cleaned Data",
        "Temps",
        "Berkeley_Earth_Temps_Cleaned.csv",
    )

    temps = pd.read_csv(temps_path)

    # Figure out which columns are the date and the temperature
    cols = list(temps.columns)

    # Date column: prefer a column literally called "dt"
    if "dt" in temps.columns:
        date_col = "dt"
    else:
        date_col = cols[0]

    # Temperature column: look for something that contains "temp"
    temp_candidates = [
        c for c in temps.columns if "temp" in c.lower() or "temperature" in c.lower()
    ]
    if temp_candidates:
        temp_col = temp_candidates[0]
    else:
        # fall back to second column
        temp_col = cols[1]

    # Rename to standard names that the rest of your code expects
    temps = temps.rename(columns={date_col: "dt", temp_col: "temperature"})

    # Parse dates and get year
    temps["dt"] = pd.to_datetime(temps["dt"], errors="coerce", dayfirst=True)
    temps = temps.dropna(subset=["dt"])
    temps["year"] = temps["dt"].dt.year

    # Aggregate to annual mean temperature
    temps_annual = (
        temps.groupby("year", as_index=False)["temperature"].mean().sort_values("year")
    )

    return temps_annual


# ---------------------------------------------------------------------
# MERGE DATASETS
# ---------------------------------------------------------------------
def build_merged_dataset(base_path: str):
    disasters_all, disasters_per_year = load_disaster_data(base_path)
    temps_annual = load_temperature_data(base_path)

    merged = pd.merge(temps_annual, disasters_per_year, on="year", how="left")
    merged["disaster_count"] = merged["disaster_count"].fillna(0).astype(int)

    return disasters_per_year, merged, disasters_all


# ---------------------------------------------------------------------
# SUMMARY STATISTICS
# ---------------------------------------------------------------------
def compute_disaster_summary(merged_df: pd.DataFrame) -> Dict[str, float]:
    counts = merged_df["disaster_count"]
    return {
        "min": int(counts.min()),
        "max": int(counts.max()),
        "mean": float(counts.mean()),
        "median": int(counts.median()),
        "std": float(counts.std()),
        "years_with_data": int(counts.count()),
    }


# ---------------------------------------------------------------------
# DISASTER TYPE COUNTS
# ---------------------------------------------------------------------
def disaster_type_counts(df: pd.DataFrame) -> pd.Series:
    return df["disaster_type"].value_counts()

