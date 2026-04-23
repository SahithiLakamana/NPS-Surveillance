# NPS Surveillance

A Reddit-based surveillance system for monitoring novel psychoactive substance (NPS) trends. This repository contains the data collection pipeline and statistical analysis scripts.
---

## Repository Structure

```
NPS-Surveillance/
├── README.md
├── data/
│   ├── keywords/
│   │   └── keywords_sample.json   # Sample keyword variants per NPS
│   └── drug_category/         # Drug Category.xlsx goes here
├── pipeline/
│   ├── top_subreddits_weekly_extraction.py
│   └── reddit_user_timelines.py
└── analysis/
    ├── trend_analysis.py
    ├── correlation_analysis.py
    └── comention_analysis.py
```

---

## Data

### Reddit Data
Post data was collected from drug-related subreddits using the PRAW
(Python Reddit API Wrapper) pipeline in `pipeline/`. The raw dataset
is not publicly distributed. 

### NFLIS Data
Annual forensic drug report counts were obtained from the DEA's National
Forensic Laboratory Information System (NFLIS).

---

## Setup

### 1. Clone the repository
```bash
git clone https://github.com/<your-username>/NPS-Surveillance.git
cd NPS-Surveillance
```

### 2. Create a virtual environment and install dependencies
```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```
---

## Pipeline

The data collection pipeline runs continuously and consists of two scripts
that should be run in parallel:

### Subreddit extraction (weekly cycle)
Extracts posts from all tracked subreddits every 7 days.
```bash
python pipeline/top_subreddits_weekly_extraction.py
```

### User timeline extraction (weekly cycle)
Extracts full submission histories for all tracked accounts and discovers
new drug-related subreddits via account activity. The pipeline retains the
capability to retrieve historical posts from newly added subreddits,
minimizing missed content as coverage expands.
```bash
python pipeline/reddit_user_timelines.py
```

Both scripts load credentials from the `.env` file and keywords from
`data/keywords/keywords_sample.json`. Replace the sample keyword file
with your full production keyword list before running.

---

## Analysis

Each analysis script accepts command-line arguments for input data paths
and output directory. Run scripts from the repository root.

### Temporal trend analysis (Mann-Kendall)
```bash
python analysis/trend_analysis.py \
    --data       data/Novel__data.csv \
    --drug_cat   data/drug_category/Drug\ Category.xlsx \
    --output_dir results/trend
```

### Reddit–NFLIS cross-correlation analysis
```bash
python analysis/correlation_analysis.py \
    --data       data/Novel__data.csv \
    --nflis      data/NFLIS/NFLIS_NPS_data_2015_2025.csv \
    --output_dir results/correlation
```

### Polysubstance co-mention analysis
```bash
python analysis/comention_analysis.py \
    --data       data/Novel__data.csv \
    --drug_cat   data/drug_category/Drug\ Category.xlsx \
    --output_dir results/comention
```

---

## Interactive Dashboard

Aggregated results are available via a publicly accessible Tableau dashboard:
https://sarkerlab.org/emerging-psychoactives-reddit/

The dashboard includes temporal posting patterns, account growth metrics,
substance co-mention frequencies, and drug co-mention network graphs.
No personally identifiable information is available from the dashboard.

---

This publication was supported by the National Institute on Drug Abuse (NIDA)
of the National Institutes of Health (NIH); award number R01DA057599.
The content is solely the responsibility of the authors and does not
necessarily represent the official views of the NIH.
