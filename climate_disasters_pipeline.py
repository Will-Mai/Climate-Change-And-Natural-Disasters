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

import os
from typing import Tuple, Dict

import pandas as pd


# ---------------------------------------------------------------------
# 1. Load disaster data  (only Baris_Dincer_Disasters_Cleaned.csv)
# ---------------------------------------------------------------------
def load_disaster_data(
    base_path: str = ".",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and clean disaster dataset.

    After the team decision, we only use:
      Cleaned Data/Natural Disasters/Baris_Dincer_Disasters_Cleaned.csv

    Returns
    -------
    disasters_all : DataFrame
        Long-format events table with columns:
        ['event_date', 'year', 'disaster_type', 'source']
    disasters_per_year : DataFrame
        Per-year counts: ['year', 'disaster_count']
    """
    # Build full path to the Baris disasters file
    dis_bar_path = os.path.join(
        base_path,
        "Cleaned Data",
        "Natural Disasters",
        "Baris_Dincer_Disasters_Cleaned.csv",
    )

    # Read the CSV
    # Expected columns include 'EventDate' and Var2–Var5 (from EM-DAT)
    dis_bar = pd.read_csv(dis_bar_path)

    # Parse date and year
    dis_bar["event_date"] = pd.to_datetime(dis_bar["EventDate"], errors="coerce")
    dis_bar["year"] = dis_bar["event_date"].dt.year

    # Rename columns so we have a clear 'disaster_type' field
    # Var5 is usually the hazard type (e.g., Wildfire, Flood, etc.)
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

    # Clean up: drop rows missing year or disaster_type and make year int
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


# ---------------------------------------------------------------------
# 2. Build merged dataset (here it's just the per-year disaster table)
# ---------------------------------------------------------------------
def build_merged_dataset(base_path: str = ".") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Build the main per-year dataset using ONLY disaster data.

    Returns
    -------
    disasters_per_year : DataFrame
        Per-year counts: ['year', 'disaster_count'].
    merged : DataFrame
        Same as disasters_per_year (kept for compatibility with app.py).
    """
    disasters_all, disasters_per_year = load_disaster_data(base_path=base_path)
    merged = disasters_per_year.copy()
    return disasters_per_year, merged


# ---------------------------------------------------------------------
# 3. Summary statistics for disaster counts per year
# ---------------------------------------------------------------------
def compute_disaster_summary(merged: pd.DataFrame) -> Dict[str, float]:
    """
    Compute summary statistics for annual disaster counts.

    Parameters
    ----------
    merged : DataFrame
        Must contain a 'disaster_count' column.

    Returns
    -------
    dict with keys: min, max, mean, median, std, years_with_data
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


# ---------------------------------------------------------------------
# 4. Counts by disaster type (for the bar chart)
# ---------------------------------------------------------------------
def disaster_type_counts(disasters_all: pd.DataFrame) -> pd.Series:
    """
    Count how many events of each disaster_type there are.

    Returns
    -------
    Pandas Series indexed by disaster_type with counts, sorted descending.
    """
    if "disaster_type" not in disasters_all.columns:
        raise KeyError("Column 'disaster_type' not found in disasters_all.")

    df = disasters_all.copy()
    df["disaster_type"] = df["disaster_type"].astype(str)

    counts = df["disaster_type"].value_counts().sort_values(ascending=False)
    return counts
