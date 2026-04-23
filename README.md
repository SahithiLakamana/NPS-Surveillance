# NPS Surveillance

A Reddit-based surveillance system for monitoring novel psychoactive substance (NPS) trends. This repository contains the data collection pipeline and statistical analysis scripts accompanying the publication:

> Lakamana S, Das S, Spadaro A, Whitman R, Perrone J, Sarker A. *Monitoring novel psychoactive substance trends on social media: Analysis of discussions and dashboard implementation.* (2025)

Funded by the National Institute on Drug Abuse (NIDA), NIH — Award R01DA057599.

---

## Repository Structure

```
NPS-Surveillance/
├── README.md
├── requirements.txt
├── .env.example               # Template for credentials and configuration
├── .gitignore
├── data/
│   ├── README.md              # Data access instructions
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
is not publicly distributed. See `data/README.md` for the expected
data schema and instructions on reproducing the dataset.

### NFLIS Data
Annual forensic drug report counts were obtained from the DEA's National
Forensic Laboratory Information System (NFLIS). NFLIS data is not publicly
redistributable and must be requested directly:

1. Visit [https://www.nflis.deadiversion.usdoj.gov](https://www.nflis.deadiversion.usdoj.gov)
2. Register for an account
3. Request drug report counts for the substances and years of interest
4. Place the exported file at `data/NFLIS/NFLIS_NPS_data_2015_2025.csv`

See `data/README.md` for the expected schema.

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

### 3. Configure credentials
```bash
cp .env.example .env
```
Edit `.env` and fill in your Reddit API credentials and MongoDB connection strings.
Reddit API credentials can be obtained by registering an application at
[https://www.reddit.com/prefs/apps](https://www.reddit.com/prefs/apps).

### 4. Set up MongoDB
The pipeline requires two MongoDB instances:
- **Primary** (`MONGO_HOST_PRIMARY`): stores all posts and user profiles
- **Meds** (`MONGO_HOST_MEDS`): stores only posts with matched NPS keywords

Install MongoDB: [https://www.mongodb.com/docs/manual/installation](https://www.mongodb.com/docs/manual/installation)

Start both instances on the ports specified in your `.env` file before
running the pipeline.

---

## Pipeline

The data collection pipeline runs continuously and consists of two scripts
that should be run in parallel:

### Subreddit extraction (weekly cycle)
Extracts posts from all tracked subreddits every 7 days.
```bash
python pipeline/top_subreddits_weekly_extraction.py
```

### User timeline extraction (10-day cycle)
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
**[Dashboard link — add URL here]**

The dashboard includes temporal posting patterns, account growth metrics,
substance co-mention frequencies, and drug co-mention network graphs.
No personally identifiable information is available from the dashboard.

---

## Citation

If you use this code or pipeline in your work, please cite:

```
Lakamana S, Das S, Spadaro A, Whitman R, Perrone J, Sarker A.
Monitoring novel psychoactive substance trends on social media:
Analysis of discussions and dashboard implementation. (2025)
```

---

## License

This project does not currently carry an open-source license.
Please contact the corresponding author for usage permissions.

**Corresponding author:**
Abeed Sarker, PhD
Department of Biomedical Informatics, Emory University School of Medicine
abeed@dbmi.emory.edu

---

## Acknowledgements

This publication was supported by the National Institute on Drug Abuse (NIDA)
of the National Institutes of Health (NIH); award number R01DA057599.
The content is solely the responsibility of the authors and does not
necessarily represent the official views of the NIH.
