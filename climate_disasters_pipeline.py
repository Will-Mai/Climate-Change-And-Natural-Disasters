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
    Load and clean disaster dataset (Baris_Dincer only).

    Returns
    -------
    disasters_all : DataFrame
        ['event_date', 'year', 'disaster_type', 'source']
    disasters_per_year : DataFrame
        ['year', 'disaster_count']
    """
    # Build full path to the Baris disasters file
    dis_bar_path = os.path.join(
        base_path,
        "Cleaned Data",
        "Natural Disasters",
        "Baris_Dincer_Disasters_Cleaned.csv",
    )

    dis_bar = pd.read_csv(dis_bar_path)

    # 1) Create event_date and year
    if "EventDate" in dis_bar.columns:
        date_col = "EventDate"
    else:
        # fallback: first column is assumed to be the date
        date_col = dis_bar.columns[0]

    dis_bar["event_date"] = pd.to_datetime(dis_bar[date_col], errors="coerce")
    dis_bar["year"] = dis_bar["event_date"].dt.year

    # 2) Work out which column is the disaster type / hazard
    hazard_col = None
    candidate_names = [
        "disaster_type",
        "DisasterType",
        "Disaster Type",
        "Disaster_Type",
        "Hazard_Type",
        "Var5",  # original EM-DAT style column
    ]
    for c in candidate_names:
        if c in dis_bar.columns:
            hazard_col = c
            break

    if hazard_col is None:
        # Fallback: last non-date column as the type
        non_date_cols = [c for c in dis_bar.columns if "date" not in c.lower()]
        hazard_col = non_date_cols[-1]

    # 3) Build a clean standardized table explicitly
    disasters_all = pd.DataFrame(
        {
            "event_date": dis_bar["event_date"],
            "year": dis_bar["year"],
            "disaster_type": dis_bar[hazard_col].astype(str),
            "source": "Baris_Dincer",
        }
    )

    # 4) Clean up: drop rows missing year or type, ensure int years
    disasters_all = disasters_all.dropna(subset=["year", "disaster_type"])
    disasters_all["year"] = disasters_all["year"].astype(int)

    # 5) Aggregate: disasters per year
    disasters_per_year = (
        disasters_all.groupby("year", as_index=False)
        .size()
        .rename(columns={"size": "disaster_count"})
        .sort_values("year")
    )

    return disasters_all, disasters_per_year


# ---------------------------------------------------------------------
# 2. Merged dataset (here it's just the per-year disaster table)
# ---------------------------------------------------------------------
def build_merged_dataset(base_path: str = ".") -> Tuple[pd.DataFrame, pd.DataFrame]:
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

