"""
comention_analysis.py

Polysubstance co-mention analysis for novel psychoactive substances (NPSs)
on Reddit, 2015-2025.

Computes:
    - Drug co-occurrence ratio per NPS (proportion of posts mentioning
      at least one additional substance)
    - Top co-mentioned substances per NPS overall and by year
    - Annual breakdown of polysubstance vs. standalone posts

Outputs:
    - co_occurrence_ratios.csv
    - top_comentions_overall.csv
    - top_comentions_by_year.csv
    - polysubstance_annual_breakdown.csv

Usage:
    python comention_analysis.py \
        --data       <path to Novel__data.csv> \
        --drug_cat   <path to Drug Category.xlsx> \
        --output_dir <path to output directory>
"""

import argparse
import os
import pandas as pd

TARGET_NPS = [
    "kratom", "xylazine", "medetomidine",
    "nitazene", "tianeptine", "bromazolam", "2cb"
]
TOP_N = 10 


def load_data(filepath: str, drug_cat_path: str) -> pd.DataFrame:
    """Load Reddit post data and merge with drug category lookup."""
    data     = pd.read_csv(filepath)
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


def compute_co_occurrence_ratios(data: pd.DataFrame) -> pd.DataFrame:
    """
    Compute drug co-occurrence ratio for each NPS.
    Defined as the proportion of posts mentioning the NPS that also
    mention at least one additional substance.
    """
    rows = []
    for drug in TARGET_NPS:
        subset      = data[data["Drug"] == drug]
        total_posts = subset["Post Id"].nunique()
        poly_posts  = subset[
            subset["poly substance posts"] == True
        ]["Post Id"].nunique()
        rows.append({
            "NPS":                   drug,
            "Total Posts":           total_posts,
            "Polysubstance Posts":   poly_posts,
            "Co-occurrence Ratio":   round(poly_posts / total_posts, 3)
                                     if total_posts > 0 else 0,
        })
    return pd.DataFrame(rows)


def compute_top_comentions(data: pd.DataFrame, drug: str,
                            top_n: int = TOP_N) -> pd.DataFrame:
    """
    Return the top N co-mentioned substances for a given NPS,
    expressed as a count and proportion of total NPS posts.
    """
    subset      = data[data["Drug"] == drug]
    total_posts = subset["Post Id"].nunique()
    poly        = subset[subset["poly substance posts"] == True]
    comention_counts = (
        poly.groupby("Drug Mentions")["Post Id"]
        .nunique()
        .sort_values(ascending=False)
        .head(top_n)
        .reset_index()
    )
    comention_counts.columns = ["Co-mentioned Substance", "Post Count"]
    comention_counts["Proportion"] = (
        comention_counts["Post Count"] / total_posts
    ).round(3)
    comention_counts.insert(0, "NPS", drug)
    return comention_counts


def compute_annual_breakdown(data: pd.DataFrame) -> pd.DataFrame:
    """
    Return annual counts of polysubstance vs. standalone posts per NPS.
    """
    rows = []
    for drug in TARGET_NPS:
        subset = data[data["Drug"] == drug]
        annual = (
            subset.groupby(["year", "poly substance posts"])["Post Id"]
            .nunique()
            .reset_index()
        )
        annual["NPS"] = drug
        rows.append(annual)
    return pd.concat(rows, ignore_index=True)


def compute_annual_top_comentions(data: pd.DataFrame,
                                   drug: str, top_n: int = TOP_N) -> pd.DataFrame:
    """
    Return top N co-mentioned substances per year for a given NPS.
    """
    subset = data[(data["Drug"] == drug) & (data["poly substance posts"] == True)]
    annual = (
        subset.groupby(["year", "Drug Mentions"])["Post Id"]
        .nunique()
        .reset_index()
    )
    annual.columns = ["year", "Co-mentioned Substance", "Post Count"]

    top_by_year = (
        annual.sort_values(["year", "Post Count"], ascending=[True, False])
        .groupby("year")
        .head(top_n)
        .reset_index(drop=True)
    )
    top_by_year.insert(0, "NPS", drug)
    return top_by_year


def main(data_path: str, drug_cat_path: str, output_dir: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    data = load_data(data_path, drug_cat_path)

    # Co-occurrence ratios
    ratios = compute_co_occurrence_ratios(data)
    ratios.to_csv(
        os.path.join(output_dir, "co_occurrence_ratios.csv"), index=False
    )
    print("\nDrug Co-occurrence Ratios:")
    print(ratios.to_string(index=False))

    # Top co-mentioned substances overall
    overall_comentions = pd.concat(
        [compute_top_comentions(data, drug) for drug in TARGET_NPS],
        ignore_index=True
    )
    overall_comentions.to_csv(
        os.path.join(output_dir, "top_comentions_overall.csv"), index=False
    )

    # Top co-mentioned substances by year
    annual_comentions = pd.concat(
        [compute_annual_top_comentions(data, drug) for drug in TARGET_NPS],
        ignore_index=True
    )
    annual_comentions.to_csv(
        os.path.join(output_dir, "top_comentions_by_year.csv"), index=False
    )

    # Annual polysubstance breakdown
    annual_breakdown = compute_annual_breakdown(data)
    annual_breakdown.to_csv(
        os.path.join(output_dir, "polysubstance_annual_breakdown.csv"), index=False
    )

    print(f"\nAll outputs saved to: {output_dir}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="NPS Polysubstance Co-mention Analysis"
    )
    parser.add_argument("--data",       required=True, help="Path to Novel__data.csv")
    parser.add_argument("--drug_cat",   required=True, help="Path to Drug Category.xlsx")
    parser.add_argument("--output_dir", required=True, help="Path to output directory")
    args = parser.parse_args()
    main(args.data, args.drug_cat, args.output_dir)
