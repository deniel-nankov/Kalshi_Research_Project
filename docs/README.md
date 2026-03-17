# Documentation — Kalshi data pipeline

This folder holds the docs you need to run and understand the **Kalshi historical and forward data pipeline**: how we pull data, where we store it, how we check that it is healthy, and how we fix common issues.

---

## Main documents

| Document | What it's for |
|----------|----------------|
| **[Kalshi Data Pipeline Summary](KALSHI_DATA_PIPELINE_SUMMARY.md)** | Start here. One place that explains the full pipeline: timeline, historical vs forward data, how the pipeline works, where we store data, why Parquet, problems we hit and how we fixed them, health checks, duplication, and a **scripts reference** (what each script does and when to run it). Plain English, with examples and diagrams. |
| **[Data schemas](SCHEMAS.md)** | Parquet file structure for markets and trades: column names, types, and example queries. Use this when you write analytics or validation. |
| **[Data validation checklist](DATA_VALIDATION_CHECKLIST.md)** | What we check for every column and across datasets, and how each check maps to the validator. Use this to understand what “healthy data” means. |
| **[Health report warnings examined](HEALTH_REPORT_WARNINGS_EXAMINED.md)** | Each WARN from the health report explained: what it means, how serious it is, what to do (including how to backfill a boundary gap), and short technical notes on causes and prevention. |

---

## Quick links by goal

- **I want to run the pipeline and understand it** → [Kalshi Data Pipeline Summary](KALSHI_DATA_PIPELINE_SUMMARY.md)
- **I need the exact column names and types** → [Data schemas](SCHEMAS.md)
- **I want to know what the validator checks** → [Data validation checklist](DATA_VALIDATION_CHECKLIST.md)
- **The health report shows WARNs and I want to fix them** → [Health report warnings examined](HEALTH_REPORT_WARNINGS_EXAMINED.md)

---

## Archive

Older or analysis-focused docs (e.g. analysis explanations, calibration, code reviews, one-time fix logs) are in **[archive/](archive/)**. They are kept for reference but are not required for running or maintaining the pipeline.
