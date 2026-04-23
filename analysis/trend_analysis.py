"""
trend_analysis.py

Mann-Kendall temporal trend analysis of Reddit post volumes for
novel psychoactive substances (NPSs), 2015-2025.

Outputs:
    - Mann-Kendall trend test results (trend direction, tau, p-value) per NPS
    - Monthly post counts per NPS (CSV)

Usage:
    python trend_analysis.py \
        --data       <path to Novel__data.csv> \
        --drug_cat   <path to Drug Category.xlsx> \
        --output_dir <path to output directory>
"""

import argparse
import os
import pandas as pd
import pymannkendall as mk
from datetime import datetime

TARGET_NPS = [
    "kratom", "xylazine", "medetomidine",
    "nitazene", "tianeptine", "bromazolam", "2cb"
]


def load_data(filepath: str, drug_cat_path: str) -> pd.DataFrame:
    """Load Reddit post data and merge with drug category lookup."""
    data = pd.read_csv(filepath)
    drug_cat = pd.read_excel(drug_cat_path)
    data["month"] = pd.to_datetime(data["Created At Date"]).dt.month
    data["year"]  = pd.to_datetime(data["Created At Date"]).dt.year
    data = data[(data["year"] >= 2015) & (data["year"] <= 2025)]
    data = data.merge(
        drug_cat[["Drug Category", "generic name"]],
        left_on="Drug Mentions",
        right_on="generic name",
        how="left"
    )
    return data


def compute_descriptive_stats(data: pd.DataFrame) -> pd.DataFrame:
    """Compute post, subreddit, and account counts per NPS."""
    stats = []
    for drug in TARGET_NPS:
        subset = data[data["Drug"] == drug]
        stats.append({
            "NPS":             drug,
            "Post Count":      subset["Post Id"].nunique(),
            "Subreddit Count": subset["Subreddit"].nunique(),
            "Account Count":   subset["Author"].nunique(),
        })
    return pd.DataFrame(stats)


def compute_monthly_counts(data: pd.DataFrame, drug: str) -> pd.Series:
    """Return monthly distinct post counts for a single NPS."""
    subset = data[data["Drug"] == drug].copy()
    subset["year-month"] = (
        subset["year"].astype(str) + "-" + subset["month"].astype(str).str.zfill(2)
    )
    counts = (
        subset.groupby("year-month")["Post Id"]
        .nunique()
        .sort_index()
    )
    return counts


def run_mann_kendall(counts: pd.Series, drug: str) -> dict:
    """
    Run Mann-Kendall trend test on monthly post counts.

    Returns a dict with trend direction, Kendall's tau, and p-value.
    """
    result = mk.original_test(counts)
    return {
        "NPS":    drug,
        "Trend":  result.trend,
        "Tau":    round(result.Tau, 3),
        "p-value": round(result.p, 4),
    }


def main(data_path: str, drug_cat_path: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    data = load_data(data_path, drug_cat_path)

    # Descriptive statistics
    desc_stats = compute_descriptive_stats(data)
    desc_stats.to_csv(os.path.join(output_dir, "descriptive_stats.csv"), index=False)
    print("\nDescriptive Statistics:")
    print(desc_stats.to_string(index=False))

    # Mann-Kendall trend tests
    mk_results = []
    for drug in TARGET_NPS:
        counts = compute_monthly_counts(data, drug)
        if len(counts) < 4:
            print(f"Insufficient data for Mann-Kendall test: {drug}")
            continue
        result = run_mann_kendall(counts, drug)
        mk_results.append(result)
        counts.to_csv(
            os.path.join(output_dir, f"{drug}_monthly_counts.csv"),
            header=["post_count"]
        )

    mk_df = pd.DataFrame(mk_results)
    mk_df.to_csv(os.path.join(output_dir, "mann_kendall_results.csv"), index=False)
    print("\nMann-Kendall Trend Test Results:")
    print(mk_df.to_string(index=False))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NPS Temporal Trend Analysis")
    parser.add_argument("--data",       required=True, help="Path to Novel__data.csv")
    parser.add_argument("--drug_cat",   required=True, help="Path to Drug Category.xlsx")
    parser.add_argument("--output_dir", required=True, help="Path to output directory")
    args = parser.parse_args()
    main(args.data, args.drug_cat, args.output_dir)
