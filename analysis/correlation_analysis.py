"""
correlation_analysis.py

Spearman cross-correlation analysis between annual Reddit post volumes
and NFLIS forensic drug report counts for five NPSs, with Bonferroni
correction across 25 comparisons (5 substances × 5 lag windows).

Lag convention:
    Negative lag (e.g., -1) : Reddit precedes NFLIS by that many years
    Zero lag (0)             : Contemporaneous comparison
    Positive lag (e.g., +1)  : NFLIS precedes Reddit by that many years

Outputs:
    - Cross-correlation results table with Bonferroni-adjusted p-values (CSV)

Usage:
    python correlation_analysis.py \
        --data       <path to Novel__data.csv> \
        --nflis      <path to NFLIS_NPS_data_2015_2025.csv> \
        --output_dir <path to output directory>
"""

import argparse
import os
import numpy as np
import pandas as pd
from scipy.stats import spearmanr
from statsmodels.stats.multitest import multipletests

# Substances with available NFLIS data
NFLIS_NPS = ["tianeptine", "medetomidine", "nitazene", "bromazolam", "xylazine"]
MAX_LAG    = 2
N_COMPARISONS = len(NFLIS_NPS) * (2 * MAX_LAG + 1)  # 5 × 5 = 25


def load_reddit_annual(data_path: str) -> pd.DataFrame:
    """Return annual distinct post counts per NPS from Reddit data."""
    data = pd.read_csv(data_path)
    data["year"] = pd.to_datetime(data["Created At Date"]).dt.year
    data = data[(data["year"] >= 2015) & (data["year"] <= 2024)]
    annual = (
        data.groupby(["Drug", "year"])["Post Id"]
        .nunique()
        .reset_index()
        .rename(columns={"Post Id": "reddit_posts"})
    )
    return annual


def load_nflis_annual(nflis_path: str) -> pd.DataFrame:
    """Return annual NFLIS drug report counts per NPS."""
    nflis = pd.read_csv(nflis_path)
    nflis["year"] = pd.to_datetime(nflis["reported_date"]).dt.year
    annual = (
        nflis.groupby(["meds_mentioned", "year"])["DRUG_REPORTS"]
        .sum()
        .reset_index()
        .rename(columns={"meds_mentioned": "Drug"})
    )
    return annual


def compute_cross_correlations(reddit_df: pd.DataFrame,
                                nflis_df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute Spearman cross-correlations at lags -MAX_LAG to +MAX_LAG
    for each NPS with available NFLIS data.

    A negative lag shifts NFLIS forward in time, which tests whether
    Reddit posts precede forensic detections.
    """
    results = []
    raw_pvals = []

    for drug in NFLIS_NPS:
        reddit_drug = reddit_df[reddit_df["Drug"] == drug]
        nflis_drug  = nflis_df[nflis_df["Drug"] == drug]

        merged = nflis_drug.merge(reddit_drug, on="year", how="left")
        merged = merged.sort_values("year").reset_index(drop=True)

        for lag in range(-MAX_LAG, MAX_LAG + 1):
            nflis_shifted = merged["DRUG_REPORTS"].shift(lag)
            rho, p = spearmanr(
                merged["reddit_posts"], nflis_shifted, nan_policy="omit"
            )
            results.append({
                "NPS":             drug,
                "Reddit Posts":    merged["reddit_posts"].sum(),
                "NFLIS Reports":   int(merged["DRUG_REPORTS"].sum()),
                "Lead/Lag":        lag,
                "Spearman (rho)":  round(rho, 2),
                "p-value (raw)":   round(p, 4),
            })
            raw_pvals.append(p)

    results_df = pd.DataFrame(results)

    # Bonferroni correction across all 25 comparisons
    reject, p_adjusted, _, _ = multipletests(
        raw_pvals, alpha=0.05, method="bonferroni"
    )
    results_df["Reject Null"] = reject
    results_df["p-adjusted"]  = p_adjusted.round(4)

    return results_df


def interpret_correlation(rho: float, reject: bool) -> str:
    """
    Classify correlation strength based on effect size (rho) and
    Bonferroni-corrected statistical significance.

    Strong   : rho > 0.80 and reject null
    Moderate : 0.50 <= rho <= 0.70 and reject null
    NS       : fail to reject null at adjusted alpha = 0.002
    """
    if not reject:
        return "NS"
    if abs(rho) > 0.80:
        return "Strong"
    if 0.50 <= abs(rho) <= 0.70:
        return "Moderate"
    return "NS"


def main(data_path: str, nflis_path: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)

    reddit_annual = load_reddit_annual(data_path)
    nflis_annual  = load_nflis_annual(nflis_path)

    results = compute_cross_correlations(reddit_annual, nflis_annual)

    # Add interpretation based on strongest correlation per NPS
    results["Interpretation"] = results.apply(
        lambda row: interpret_correlation(row["Spearman (rho)"], row["Reject Null"]),
        axis=1
    )

    output_path = os.path.join(output_dir, "cross_correlation_results.csv")
    results.to_csv(output_path, index=False)

    print(f"\nBonferroni correction: alpha = 0.05 / {N_COMPARISONS} = "
          f"{0.05 / N_COMPARISONS:.4f}")
    print("\nCross-Correlation Results (Reddit vs NFLIS):")
    print(results.to_string(index=False))
    print(f"\nResults saved to: {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NPS Reddit-NFLIS Cross-Correlation Analysis"
    )
    parser.add_argument("--data",       required=True, help="Path to Novel__data.csv")
    parser.add_argument("--nflis",      required=True, help="Path to NFLIS CSV")
    parser.add_argument("--output_dir", required=True, help="Path to output directory")
    args = parser.parse_args()
    main(args.data, args.nflis, args.output_dir)
