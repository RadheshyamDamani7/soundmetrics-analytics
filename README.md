# SoundMetrics — Music Streaming Analytics

![Python](https://img.shields.io/badge/Python-3.13-blue)
![MySQL](https://img.shields.io/badge/MySQL-8.0-orange)
![scikit--learn](https://img.shields.io/badge/scikit--learn-ML-green)
![Power BI](https://img.shields.io/badge/Power%20BI-Dashboard-yellow)

## Overview
End-to-end analytics project on a B2C music streaming platform with
**1.47M rows** across 5 relational tables. Covers the full analytics
pipeline from data engineering to machine learning to business recommendations.

## Key Results
| Metric | Value |
|---|---|
| Total Users Analysed | 99,500 |
| Total Revenue Modelled | $20.75M |
| Paid Conversion Rate | 60.72% |
| ML Clusters Identified | 5 |
| Revenue Opportunities Found | $2.3M |

## Tech Stack
- **Python** — pandas, numpy, matplotlib, seaborn, scikit-learn, Faker
- **SQL** — MySQL 8.0, CTEs, window functions, multi-table joins
- **Machine Learning** — K-Means clustering, PCA, RFM scoring
- **Visualisation** — Power BI (4-page dashboard), matplotlib, seaborn

## Project Structure

soundmetrics/
├── notebooks/
│   ├── 02_data_cleaning.ipynb   # Data cleaning pipeline
│   ├── 03_eda.ipynb             # 11 EDA charts
│   └── 05_ml_segmentation.ipynb # RFM + K-Means ML
├── sql/
│   ├── 01_create_schema.sql     # Database schema
│   ├── 03_business_queries.sql  # 17 business queries
│   └── 04_segmentation_fixed.sql# RFM SQL segmentation
├── scripts/
│   ├── generate_data_v2.py      # Synthetic data generation
│   └── 02_load_FINAL.py         # MySQL data loader
├── data/charts/                 # All EDA and ML charts
└── report/                      # Full analytics report

## Phases
| Phase | Description | Tools |
|---|---|---|
| 1 — Data Generation | 1.47M rows across 5 tables | Python, Faker |
| 2 — Data Cleaning | Null handling, type fixing, deduplication | pandas |
| 3 — EDA | 11 business charts | matplotlib, seaborn |
| 4 — SQL Analysis | 17 queries, RFM scoring | MySQL 8.0 |
| 5 — Machine Learning | K-Means (K=5), PCA, RFM | scikit-learn |
| 6 — Dashboard | 4-page interactive dashboard | Power BI |

## Key Findings
- **High-Value At Risk** cluster: 5,064 users averaging $394 revenue each but 1.06 failed payments — $2M+ in recoverable revenue
- **Disengaged Users** cluster: 61.8% skip rate vs 18% platform average — signals recommendation engine failure
- **Dormant Users**: 1,012 days inactive but longest session duration of all clusters — strong win-back campaign candidates
- **Word of Mouth** has highest paid conversion rate at 61.9% — higher than all paid advertising channels

## ML Methodology
- **Features:** 9 behavioural metrics per user
- **Preprocessing:** StandardScaler normalisation
- **K Selection:** Elbow Method + Silhouette Score (K=2 to K=9)
- **Final Model:** K=5 (Silhouette Score = 0.2268)
- **Validation:** PCA visualisation (45.5% variance retained in 2D)

## How to Run

```bash
# 1. Install dependencies
pip install pandas numpy matplotlib seaborn scikit-learn faker mysql-connector-python

# 2. Generate data
python scripts/generate_data_v2.py

# 3. Clean data
# Run notebooks/02_data_cleaning.ipynb

# 4. Load to MySQL
# Update DB_CONFIG in scripts/02_load_FINAL.py with your credentials
python scripts/02_load_FINAL.py

# 5. Run ML notebook
# Run notebooks/05_ml_segmentation.ipynb
```

## Author
**Radheshyam Damani**
MS Information Systems — Stevens Institute of Technology
[LinkedIn](https://www.linkedin.com/in/radheshyam-damani-data) | [Email](mailto:radhedamani7@gmail.com)
