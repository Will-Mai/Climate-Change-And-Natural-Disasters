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

# Folder layout (what you showed in the screenshot):
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






def load_temperature_data(
    base_path: str = ".",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and clean temperature datasets.
    """
    # These names must match exactly what is in Cleaned Data/Temps
    temps_gia_path   = _temp_path(base_path, "Gia_Bách_Nguyễn_Earth_Temps_Cleaned.csv")
    temps_berk_path  = _temp_path(base_path, "Berkeley_Earth_Temps_Cleaned.csv")
    temps_josep_path = _temp_path(base_path, "Josep_Ferrer_Temps_Cleaned.csv")





    # --- Gia: annual averages in Fahrenheit ---
    temps_gia = pd.read_csv(temps_gia_path)
    temps_gia = temps_gia.rename(
        columns={
            "Year": "year",
            "Average_Fahrenheit_Temperature": "TempF",
        }
    )
    temps_gia["source"] = "Gia_Bách_Nguyễn"

    # --- Berkeley Earth: monthly temps in °C -> °F ---
    temps_berk = pd.read_csv(temps_berk_path)

    # Handle date column (dt or Date)
    if "dt" in temps_berk.columns:
        date_col = "dt"
    elif "Date" in temps_berk.columns:
        date_col = "Date"
    else:
        # Fall back: first non-numeric column as date
        non_numeric = temps_berk.select_dtypes(exclude="number").columns
        date_col = non_numeric[0]

    temps_berk["date"] = pd.to_datetime(temps_berk[date_col], errors="coerce")
    temps_berk["year"] = temps_berk["date"].dt.year

    # Detect which column contains temperature data
    temp_candidates = [
        "LandAndOceanAverageTemperature",
        "LandAverageTemperature",
        "AverageTemperature",
        "TemperatureC",
        "Temperature",
    ]
    temp_col = None
    for c in temp_candidates:
        if c in temps_berk.columns:
            temp_col = c
            break

    # If none of the expected names are present, pick the first numeric column
    if temp_col is None:
        numeric_cols = temps_berk.select_dtypes(include="number").columns.tolist()
        if not numeric_cols:
            raise KeyError(
                f"No numeric temperature column found in Berkeley dataset. "
                f"Available columns: {list(temps_berk.columns)}"
            )
        temp_col = numeric_cols[0]

    # Convert to Fahrenheit (Berkeley data is in °C)
    temps_berk["TempF"] = temps_berk[temp_col] * 9 / 5 + 32
    temps_berk["source"] = "Berkeley_Earth"


    # --- Josep Ferrer: monthly temps in °F by country ---
    temps_josep = pd.read_csv(temps_josep_path)
    temps_josep["date"] = pd.to_datetime(temps_josep["EventDate"], errors="coerce")
    temps_josep["year"] = temps_josep["date"].dt.year
    temps_josep["TempF"] = pd.to_numeric(
        temps_josep["TemperatureFahrenheit"], errors="coerce"
    )
    temps_josep["source"] = "Josep_Ferrer"

    # Combine all temperature sources
    temps_all = pd.concat(
        [
            temps_gia[["year", "TempF", "source"]],
            temps_berk[["year", "TempF", "source"]],
            temps_josep[["year", "TempF", "source"]],
        ],
        ignore_index=True,
    )

    temps_all = temps_all.dropna(subset=["year", "TempF"])
    temps_all["year"] = temps_all["year"].astype(int)

    temps_annual = (
        temps_all.groupby("year", as_index=False)["TempF"]
        .mean()
        .sort_values("year")
    )

    return temps_all, temps_annual



def load_disaster_data(
    base_path: str = ".",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Load and clean disaster datasets.

    Returns:
        disasters_all: long-format events table with columns
                       ['event_date', 'year', 'disaster_type', 'source']
        disasters_per_year: aggregated table ['year', 'disaster_count']
    """
    # File paths inside Cleaned Data/Natural Disasters
    dis_bar_path = _disaster_path(base_path, "Baris_Dincer_Disasters_Cleaned.csv")
    dis_shrey_path = _disaster_path(base_path, "Shreyansh_Dangi_Disasters_Cleaned.csv")

    # ---------- Baris Dinçer disasters ----------
    dis_bar = pd.read_csv(dis_bar_path)

    # Prefer an existing Year column if it exists
    if "Year" in dis_bar.columns:
        dis_bar["year"] = pd.to_numeric(dis_bar["Year"], errors="coerce")
        dis_bar["event_date"] = pd.to_datetime(
            dis_bar.get("EventDate", pd.NaT), errors="coerce"
        )
    else:
        # Parse dates and derive year
        if "EventDate" in dis_bar.columns:
            date_col = "EventDate"
        else:
            non_numeric = dis_bar.select_dtypes(exclude="number").columns
            date_col = non_numeric[0]

        dis_bar["event_date"] = pd.to_datetime(dis_bar[date_col], errors="coerce")
        dis_bar["year"] = dis_bar["event_date"].dt.year

    # Figure out which column holds the disaster type
    baris_type_col = None

    # 1) many versions use Var5
    if "Var5" in dis_bar.columns:
        baris_type_col = "Var5"

    # 2) otherwise, look for likely names
    if baris_type_col is None:
        for c in ["disaster_type", "DisasterType", "Disaster_Type", "Disaster Type"]:
            if c in dis_bar.columns:
                baris_type_col = c
                break

    # 3) ultimate fallback: last column
    if baris_type_col is None:
        baris_type_col = dis_bar.columns[-1]

    # Create a unified disaster_type column
    dis_bar["disaster_type"] = dis_bar[baris_type_col]
    dis_bar["source"] = "Baris_Dincer"
    dis_bar_std = dis_bar[["event_date", "year", "disaster_type", "source"]]

    # ---------- Shreyansh Dangi disasters ----------
    dis_shrey = pd.read_csv(dis_shrey_path)

    # Prefer an existing Year column if it exists
    if "Year" in dis_shrey.columns:
        dis_shrey["year"] = pd.to_numeric(dis_shrey["Year"], errors="coerce")
        dis_shrey["event_date"] = pd.to_datetime(
            dis_shrey.get("Date", pd.NaT), errors="coerce"
        )
    else:
        if "Date" in dis_shrey.columns:
            s_date_col = "Date"
        else:
            non_numeric_s = dis_shrey.select_dtypes(exclude="number").columns
            s_date_col = non_numeric_s[0]

        dis_shrey["event_date"] = pd.to_datetime(
            dis_shrey[s_date_col], errors="coerce"
        )
        dis_shrey["year"] = dis_shrey["event_date"].dt.year

    # Detect disaster-type column
    shrey_type_col = None
    for c in ["DisasterType", "disaster_type", "Disaster_Type", "Disaster Type"]:
        if c in dis_shrey.columns:
            shrey_type_col = c
            break
    if shrey_type_col is None:
        shrey_type_col = dis_shrey.columns[-1]

    dis_shrey["disaster_type"] = dis_shrey[shrey_type_col]
    dis_shrey["source"] = "Shreyansh_Dangi"
    dis_shrey_std = dis_shrey[["event_date", "year", "disaster_type", "source"]]

    # ---------- Combine both sources ----------
    disasters_all = pd.concat([dis_bar_std, dis_shrey_std], ignore_index=True)

    # Clean missing / bad years
    disasters_all = disasters_all.dropna(subset=["year", "disaster_type"])
    disasters_all["year"] = disasters_all["year"].astype(int)

    # Keep only realistic years (adjust max as you like: 2025, 2030, etc.)
    disasters_all = disasters_all[
        (disasters_all["year"] >= 1900) & (disasters_all["year"] <= 2025)
    ]

    # Aggregate disasters per year
    disasters_per_year = (
        disasters_all.groupby("year", as_index=False)
        .size()
        .rename(columns={"size": "disaster_count"})
        .sort_values("year")
    )

    return disasters_all, disasters_per_year





# --------------------------------------------------------------------
# 2. Merged dataset + helpers
# --------------------------------------------------------------------

def build_merged_dataset(
    base_path: str = ".",
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Convenience function to load everything and build the merged annual dataset.

    Returns:
        temps_annual:        ['year', 'TempF']
        disasters_per_year:  ['year', 'disaster_count']
        merged:              ['year', 'TempF', 'disaster_count']
    """
    _, temps_annual = load_temperature_data(base_path=base_path)
    _, disasters_per_year = load_disaster_data(base_path=base_path)

    merged = pd.merge(
        temps_annual, disasters_per_year, on="year", how="outer"
    ).sort_values("year")

    return temps_annual, disasters_per_year, merged


def compute_disaster_summary(merged: pd.DataFrame) -> Dict[str, float]:
    """
    Compute summary statistics for disaster_count (like Total Spent stats).

    Returns a dict with keys:
        Count, Mean, StdDev, Min, Median, Max, Sum
    """
    dc = merged["disaster_count"]

    summary_stats = {
        "Count": float(dc.count()),
        "Mean": float(dc.mean()),
        "StdDev": float(dc.std()),
        "Min": float(dc.min()),
        "Median": float(dc.median()),
        "Max": float(dc.max()),
        "Sum": float(dc.sum()),
    }
    return summary_stats


def disaster_type_counts(disasters_all: pd.DataFrame) -> pd.Series:
    """
    Return a Series with counts per disaster_type.
    """
    return disasters_all["disaster_type"].value_counts()


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
