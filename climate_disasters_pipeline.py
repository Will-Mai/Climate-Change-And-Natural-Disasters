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
    """Loads monthly temperature data and converts to annual average."""

    temp_path = os.path.join(
        base_path, "Cleaned Data", "Temps", "Berkeley_Earth_Temps_Cleaned.csv"
    )

    temps = pd.read_csv(temp_path)

    temps.columns = ["dt", "temperature"]
    temps["dt"] = pd.to_datetime(temps["dt"], errors="coerce")
    temps["year"] = temps["dt"].dt.year

    temps_annual = (
        temps.groupby("year")["temperature"]
        .mean()
        .reset_index(name="TempF")
    )

    # Limit to 1970–2022 to match disasters
    temps_annual = temps_annual[(temps_annual["year"] >= 1970) & (temps_annual["year"] <= 2022)]

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

