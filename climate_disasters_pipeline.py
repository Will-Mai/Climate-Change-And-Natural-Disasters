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


def load_disaster_data(base_path: str):
    dis_path = os.path.join(base_path, "Cleaned Data", "Natural Disasters", "Baris_Dincer_Disasters_Cleaned.csv")

    df = pd.read_csv(dis_path)

    # Fix column names
    df.columns = ["event_date", "region", "category", "subcategory", "disaster_type"]

    # Parse dates
    df["event_date"] = pd.to_datetime(df["event_date"], errors="coerce")
    df["year"] = df["event_date"].dt.year

    # Drop empty years
    df = df.dropna(subset=["year"])
    df["year"] = df["year"].astype(int)

    # Use Var5 as the true disaster type
    df["disaster_type"] = df["disaster_type"].astype(str).str.strip()

    disasters_per_year = df.groupby("year").size().reset_index(name="disaster_count")

    return df, disasters_per_year


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
    merged = disasters_per_year[(disasters_per_year["year"] >= 1970) &
                                (disasters_per_year["year"] <= 2022)]
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


def disaster_type_counts(df):
    return df["disaster_type"].value_counts()


