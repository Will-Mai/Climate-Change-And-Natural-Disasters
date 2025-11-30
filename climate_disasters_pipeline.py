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

from __future__ import annotations

import os
from typing import Tuple, Dict

import numpy as np
import pandas as pd

def _guess_disaster_type_column(df: pd.DataFrame) -> str:
    """
    Try to guess which column contains the disaster type.

    We prefer more detailed fields first (subtype / subsubtype),
    then broader 'type' fields, then fall back to a generic object column.
    """
    object_cols = list(df.select_dtypes(include="object").columns)

    if not object_cols:
        # No object columns -> fall back to last column
        return df.columns[-1]

    # 1) Prefer *fine-grained* subtype / subsubtype columns if present
    fine_names = [
        "Disaster Subsubtype",
        "Disaster_Subsubtype",
        "DisasterSubsubtype",
        "Disaster Subtype",
        "Disaster_Subtype",
        "DisasterSubtype",
        "Subsubtype",
        "SubSubType",
        "Subtype",
        "SubType",
    ]
    for name in fine_names:
        if name in df.columns:
            return name

    # 2) Then explicit "type" / "hazard" columns (coarser categories)
    coarse_names = [
        "disaster_type",
        "DisasterType",
        "Disaster_Type",
        "Disaster Type",
        "Hazard_Type",
        "Hazard Type",
    ]
    for name in coarse_names:
        if name in df.columns:
            return name

    # 3) Any object column with useful keywords, preferring subtype-like names
    keywords = ["subsubtype", "subtype", "disaster", "hazard", "eventtype", "event_type", "type"]
    for col in object_cols:
        low = col.lower().replace(" ", "")
        if any(k in low for k in keywords):
            return col

    # 4) Fallback: first object col that isn't clearly a date/year/id
    for col in object_cols:
        low = col.lower()
        if "year" in low or "date" in low or "time" in low or "id" in low:
            continue
        return col

    # 5) Final fallback: last column
    return df.columns[-1]



#   Cleaned Data/
#       Temps/
#       Natural Disasters/
BASE_DATA_DIR = "Cleaned Data"
TEMPS_DIR = os.path.join(BASE_DATA_DIR, "Temps")
DISASTERS_DIR = os.path.join(BASE_DATA_DIR, "Natural Disasters")


def _temp_path(base_path: str, filename: str) -> str:
    """Full path to a temperature CSV file."""
    return os.path.join(base_path, TEMPS_DIR, filename)


def _disaster_path(base_path: str, filename: str) -> str:
    """Full path to a natural-disaster CSV file."""
    return os.path.join(base_path, DISASTERS_DIR, filename)





def load_disaster_data(
    base_path: str = ".",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and clean disaster dataset.

    After team decision, we only use:
    Cleaned Data/Natural Disasters/Baris_Dincer_Disasters_Cleaned.csv

    Returns:
        disasters_all: long-format events table
                       ['event_date', 'year', 'disaster_type', 'source']
        disasters_per_year: aggregated table ['year', 'disaster_count']
    """
    # Build the full path directly, no _csv_path helper
    dis_bar_path = os.path.join(
        base_path,
        "Cleaned Data",
        "Natural Disasters",
        "Baris_Dincer_Disasters_Cleaned.csv",
    )

    # --- Baris Dinçer disasters ---
    # expected columns: ['EventDate', 'Var2', 'Var3', 'Var4', 'Var5']
    dis_bar = pd.read_csv(dis_bar_path)

    # Parse date and year
    dis_bar["event_date"] = pd.to_datetime(dis_bar["EventDate"], errors="coerce")
    dis_bar["year"] = dis_bar["event_date"].dt.year

    # Rename columns so we have a clear disaster_type field
    dis_bar = dis_bar.rename(
        columns={
            "Var2": "region",
            "Var3": "disaster_group",
            "Var4": "disaster_subgroup",
            "Var5": "disaster_type",
        }
    )
    dis_bar["source"] = "Baris_Dincer"

    # Standardized event table
    disasters_all = dis_bar[["event_date", "year", "disaster_type", "source"]].copy()

    # Drop rows missing year or disaster_type
    disasters_all = disasters_all.dropna(subset=["year", "disaster_type"])
    disasters_all["year"] = disasters_all["year"].astype(int)

    # Aggregate: disasters per year
    disasters_per_year = (
        disasters_all.groupby("year", as_index=False)
        .size()
        .rename(columns={"size": "disaster_count"})
        .sort_values("year")
    )

    return disasters_all, disasters_per_year





def load_temperature_data(
    base_path: str = ".",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and clean temperature dataset.

    After team decision, we only use the Berkeley Earth dataset:
    Cleaned Data/Temps/Berkeley_Earth_Temps_Cleaned.csv

    Returns:
        temps_all:   long-format table ['year', 'TempF', 'source']
        temps_annual: annual averages ['year', 'TempF']
    """
    # Path to the Berkeley file in the repo
    temps_berk_path = _csv_path(
        base_path,
        os.path.join("Cleaned Data", "Temps", "Berkeley_Earth_Temps_Cleaned.csv"),
    )

    # --- Berkeley Earth: monthly temps in °C -> convert to °F and create 'year' ---
    # expected columns: ['dt', 'LandAndOceanAverageTemperature']
    temps_berk = pd.read_csv(temps_berk_path)
    temps_berk["date"] = pd.to_datetime(temps_berk["dt"], errors="coerce")
    temps_berk["year"] = temps_berk["date"].dt.year
    temps_berk["TempF"] = temps_berk["LandAndOceanAverageTemperature"] * 9.0 / 5.0 + 32.0
    temps_berk["source"] = "Berkeley_Earth"

    # Use only the needed columns
    temps_all = temps_berk[["year", "TempF", "source"]].copy()

    # Clean: drop rows with missing year or TempF
    temps_all = temps_all.dropna(subset=["year", "TempF"])
    temps_all["year"] = temps_all["year"].astype(int)

    # Annual average TempF
    temps_annual = (
        temps_all.groupby("year", as_index=False)["TempF"]
        .mean()
        .sort_values("year")
    )

    return temps_all, temps_annual



# --------------------------------------------------------------------
# 2. Merged dataset + helpers
# --------------------------------------------------------------------

def build_merged_dataset(base_path: str = ".") -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build the main per-year dataset using ONLY disaster data.
    """
    disasters_all, disasters_per_year = load_disaster_data(base_path=base_path)
    merged = disasters_per_year.copy()
    return disasters_per_year, merged




def disaster_type_counts(disasters_all: pd.DataFrame) -> pd.Series:
    """
    Count how many events of each disaster_type there are.

    Returns:
        Pandas Series indexed by disaster_type with counts.
    """
    if "disaster_type" not in disasters_all.columns:
        raise KeyError("Column 'disaster_type' not found in disasters_all.")

    # Ensure everything is treated as a string (fixes ArrowTypeError)
    df = disasters_all.copy()
    df["disaster_type"] = df["disaster_type"].astype(str)

    counts = df["disaster_type"].value_counts().sort_values(ascending=False)
    return counts



# Optional quick test if someone runs this module directly
if __name__ == "__main__":
    base = "."

    temps_all_df, temps_annual_df = load_temperature_data(base)
    disasters_all_df, disasters_per_year_df = load_disaster_data(base)
    temps_annual_df, disasters_per_year_df, merged_df = build_merged_dataset(base)
    stats = compute_disaster_summary(merged_df)
    type_counts = disaster_type_counts(disasters_all_df)

    print("Temperature annual head:")
    print(temps_annual_df.head(), "\n")

    print("Disasters per year head:")
    print(disasters_per_year_df.head(), "\n")

    print("Merged head:")
    print(merged_df.head(), "\n")

    print("Summary stats:", stats, "\n")
    print("Top disaster types:")
    print(type_counts.head())

def compute_disaster_summary(merged: pd.DataFrame) -> Dict[str, float]:
    """
    Compute summary statistics for annual disaster counts.

    Args:
        merged: DataFrame with at least ['year', 'disaster_count'].

    Returns:
        Dictionary with min, max, mean, median, std, and number of years.
    """
    if "disaster_count" not in merged.columns:
        raise KeyError("Column 'disaster_count' not found in merged dataset.")

    counts = merged["disaster_count"].dropna()

    if counts.empty:
        return {
            "min": 0.0,
            "max": 0.0,
            "mean": 0.0,
            "median": 0.0,
            "std": 0.0,
            "years_with_data": 0,
        }

    return {
        "min": float(counts.min()),
        "max": float(counts.max()),
        "mean": float(counts.mean()),
        "median": float(counts.median()),
        "std": float(counts.std(ddof=1)),
        "years_with_data": int(counts.count()),
    }
