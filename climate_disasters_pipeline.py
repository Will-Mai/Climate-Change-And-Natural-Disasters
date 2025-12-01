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

from __future__ import annotations

import os
from typing import Tuple, Dict

import pandas as pd

DATA_DIR = os.path.join("Cleaned Data", "Natural Disasters")


def _csv_path(base_path: str, filename: str) -> str:
    return os.path.join(base_path, DATA_DIR, filename)


def load_disaster_data(base_path: str = ".") -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Load, clean, and standardize the Baris Dinçer disaster dataset."""
    dis_bar_path = _csv_path(base_path, "Baris_Dincer_Disasters_Cleaned.csv")

    dis_bar = pd.read_csv(dis_bar_path)

    # Convert date + year
    dis_bar["event_date"] = pd.to_datetime(dis_bar["EventDate"], errors="coerce")
    dis_bar["year"] = dis_bar["event_date"].dt.year
    dis_bar = dis_bar.dropna(subset=["year"])
    dis_bar["year"] = dis_bar["year"].astype(int)

    # Limit strange future entries
    dis_bar = dis_bar[(dis_bar["year"] >= 1900) & (dis_bar["year"] <= 2022)]

    # Rename columns to human-readable names
    dis_bar.rename(
        columns={
            "Var2": "region",
            "Var3": "disaster_group",
            "Var4": "broad_type",
            "Var5": "disaster_type",   # <-- REAL detailed hazard label
        },
        inplace=True,
    )

    # Full event-level table
    disasters_all = dis_bar[["event_date", "year", "disaster_type"]].copy()
    disasters_all["source"] = "Baris_Dincer"

    # Annual totals
    disasters_per_year = (
        disasters_all.groupby("year", as_index=False)
        .agg(disaster_count=("event_date", "count"))
        .sort_values("year")
    )

    return disasters_all, disasters_per_year


def build_merged_dataset(base_path: str = "."):
    disasters_all, disasters_per_year = load_disaster_data(base_path)
    merged = disasters_per_year.copy()
    return disasters_per_year, merged


def compute_disaster_summary(merged: pd.DataFrame) -> Dict[str, float]:
    s = merged["disaster_count"]
    return {
        "min": int(s.min()),
        "max": int(s.max()),
        "mean": float(s.mean()),
        "median": float(s.median()),
        "std": float(s.std()),
        "years_with_data": int(s.count()),
    }


def disaster_type_counts(disasters_all: pd.DataFrame):
    return disasters_all["disaster_type"].value_counts().sort_values(ascending=False)



