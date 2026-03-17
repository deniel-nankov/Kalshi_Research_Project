# Prediction Market Analysis

[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

A comprehensive framework for analyzing prediction market data with 19 built-in statistical analyses. Fetch real historical data from Kalshi's API and generate publication-quality figures, statistics, and insights.

## 🎯 Features

- **Real Data Fetching**: Direct integration with Kalshi's historical API
- **19 Statistical Analyses**: Professional-grade market efficiency studies
- **Publication Quality**: High-resolution figures (1200 DPI) with PDF/PNG/CSV/JSON outputs
- **Organized Results**: Automatic organization into analysis-specific folders
- **Parquet Storage**: Efficient columnar data format with DuckDB integration
- **Extensible Framework**: Easy to add custom analyses

## 📊 What's Analyzed

Our framework includes 19 comprehensive analyses across 5 categories:

### Market Efficiency
- **Calibration Deviation** - How accurate are market prices at predicting outcomes?
- **Mispricing by Price** - Where do systematic errors occur?
- **Statistical Tests** - Hypothesis testing for market efficiency claims

### Maker vs Taker Dynamics
- **Returns Comparison** - Who makes more money: patient makers or aggressive takers?
- **Win Rate Analysis** - Success rates by role and direction
- **Gap Over Time** - Evolution of the maker-taker spread
- **Returns by Category** - Performance across market types

### Price Patterns
- **YES vs NO Asymmetry** - Do NO contracts systematically outperform YES?
- **Longshot Bias** - Volume concentration in extreme predictions
- **Win Rate by Price** - Calibration at different probability levels
- **Expected Value Analysis** - Identifying profitable price ranges

### Trading Behavior
- **Volume Over Time** - Trading activity trends
- **Returns by Hour** - Intraday patterns
- **VWAP by Hour** - Time-of-day price discovery
- **Trade Size Analysis** - Position sizing by role
- **Win Rate by Trade Size** - Does bigger mean smarter?

### Market Structure
- **Meta Statistics** - Dataset overview and summary metrics
- **Market Types** - Volume distribution across categories

## 🚀 Quick Start

### Installation

Requires Python 3.9+ and [uv](https://github.com/astral-sh/uv):

### Installation

Requires Python 3.9+ and [uv](https://github.com/astral-sh/uv):

```bash
# Clone repository
git clone https://github.com/jon-becker/prediction-market-analysis.git
cd prediction-market-analysis

# Install dependencies
uv sync
```

### Two-Step Workflow

#### 1️⃣ Pull Full Historical Data

```bash
uv run python scripts/download_historical.py --clean
```

If interrupted, resume safely:

```bash
uv run python scripts/download_historical.py --resume
```

This downloads the full historical snapshot from Kalshi production endpoints into Parquet:

- `data/kalshi/historical/markets.parquet`
- `data/kalshi/historical/trades/batch_*.parquet`
- `data/kalshi/historical/.checkpoint.json`

#### 1.5️⃣ Keep Forward Ingestion Running (Incremental)

After the full historical backfill, pull new data from Kalshi and append to the dataset:

```bash
uv run python scripts/update_forward.py
```

All Kalshi data lives in one place under `data/kalshi/historical/`: historical batches plus forward-appended files.

Safe testing mode (no writes):

```bash
uv run python scripts/update_forward.py --dry-run
```

Forward (incremental) outputs — stored under the same tree as historical:

- `data/kalshi/historical/forward_trades/dt=*/trades_*.parquet`
- `data/kalshi/historical/forward_markets/dt=*/markets_*.parquet`
- `data/kalshi/state/forward_checkpoint.json`
- `data/kalshi/state/runs/*.json`

One-command validation (boundary + duplicate checks):

```bash
uv run python scripts/validate_forward_pipeline.py --strict
```

#### 2️⃣ Run All Analyses

```bash
python run_all_analyses.py
```

This converts CSVs to Parquet format and runs all 19 analyses via the built-in framework.

**Output:** 71 files (19 PNG + 19 PDF + 17 CSV + 17 JSON) in `analysis_results/`

Optional: `python organize_results.py` to group outputs into analysis-specific folders.

### Using the Built-in CLI

Alternatively, use the interactive CLI:

```bash
# Data collection
python main.py index

# Run specific analysis
python main.py analyze maker_vs_taker_returns

# Run all analyses
python main.py analyze --all
```

## 📁 Project Structure

```
prediction-market-analysis/
├── src/
│   ├── analysis/           # Analysis implementations
│   │   ├── kalshi/         # 19 Kalshi-specific analyses
│   │   └── comparison/     # Cross-platform comparisons
│   ├── indexers/           # Data fetchers
│   │   ├── kalshi/         # Kalshi API client
│   │   └── polymarket/     # Polymarket indexers
│   └── common/             # Shared utilities
│       ├── analysis.py     # Base Analysis class
│       ├── storage.py      # Parquet I/O
│       └── interfaces/     # Chart configs
│
├── data/
│   └── kalshi/
│       └── historical/
│           ├── markets.parquet
│           ├── trades/     # batch_*.parquet
│           └── .checkpoint.json
│
├── analysis_results/       # Organized outputs
│   ├── README.md           # Navigation guide
│   └── [19 analysis folders with PNG/PDF/CSV/JSON]
│
├── docs/                   # Documentation
│   ├── README.md           # Documentation hub
│   ├── ANALYSIS_EXPLANATIONS.md     # Plain English explanations
│   ├── ANALYSIS_CODE_REVIEW.md      # Code quality assessment
│   ├── SIMPLE_EXPLANATION_OF_CHANGES.md  # What we modified
│   └── [More detailed docs]
│
├── scripts/download_historical.py  # Full historical data pipeline
├── run_all_analyses.py             # Run analyses
├── organize_results.py             # Optional result organization
└── main.py                     # Built-in CLI
```

## 📚 Documentation

- **[Documentation Hub](docs/README.md)** - Start here for all documentation
- **[Analysis Explanations](docs/ANALYSIS_EXPLANATIONS.md)** - What each analysis does (plain English)
- **[Code Review](docs/ANALYSIS_CODE_REVIEW.md)** - Professional code quality assessment
- **[Changes Made](docs/SIMPLE_EXPLANATION_OF_CHANGES.md)** - What we modified from original repo
- **[Data Schemas](docs/SCHEMAS.md)** - Parquet file structure
- **[Writing Custom Analyses](docs/ANALYSIS.md)** - Developer guide

## 🔬 Sample Results

Based on 2,000 finalized Kalshi markets (March 2025):

| Metric | Value |
|--------|-------|
| **Markets Analyzed** | 2,000 finalized markets |
| **Trades** | 1,516 trades |
| **Total Volume** | $75,818 USD |
| **Calibration Deviation** | 7.88% mean absolute error |
| **Maker Advantage** | +2.3% excess returns vs takers |
| **YES/NO Asymmetry** | NO contracts outperform 67% of price bins |

See [analysis_results/README.md](analysis_results/README.md) for detailed results navigation.

## 🛠️ Technical Details

**Data Pipeline:**
1. Pull full historical data from Kalshi API to Parquet (`data/kalshi/historical/`)
2. Run DuckDB SQL analyses over Parquet
3. Generate matplotlib figures + pandas DataFrames
4. Export to PNG (1200 DPI), PDF, CSV, JSON

**Key Technologies:**
- **DuckDB**: In-memory SQL analytics on Parquet files
- **PyArrow**: Efficient Parquet I/O
- **pandas**: Data manipulation
- **matplotlib**: Publication-quality figures
- **scipy**: Statistical hypothesis testing
- **requests**: HTTP API client

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

To add a new analysis:

1. Extend the `Analysis` base class in `src/common/analysis.py`
2. Implement `run()` method returning `AnalysisOutput(figure, data, chart)`
3. Place in `src/analysis/kalshi/` or `src/analysis/polymarket/`
4. Register in `main.py` CLI

See [docs/ANALYSIS.md](docs/ANALYSIS.md) for detailed developer guide.

## 📄 License

This project is licensed under the MIT License - see [LICENSE](LICENSE) file for details.

## 🔗 Research & Citations

- Becker, J. (2026). _The Microstructure of Wealth Transfer in Prediction Markets_. [jbecker.dev/research](https://jbecker.dev/research/prediction-market-microstructure)
- Le, N. A. (2026). _Decomposing Crowd Wisdom: Domain-Specific Calibration Dynamics in Prediction Markets_. [arXiv:2602.19520](https://arxiv.org/abs/2602.19520)

If you use this framework or dataset in your research, please reach out via [email](mailto:jonathan@jbecker.dev) or [Twitter](https://x.com/BeckerrJon). We'd love to hear about your work!

## ⭐ Acknowledgments

Original framework developed by [Jonathan Becker](https://github.com/jon-becker). 

This implementation adapts the original codebase to work with Kalshi's historical API, with modifications to:
- Add timestamp type casting for Parquet VARCHAR columns
- Calculate USD volume from contract counts
- Handle small datasets with adaptive thresholds
- Add comprehensive documentation

See [docs/SIMPLE_EXPLANATION_OF_CHANGES.md](docs/SIMPLE_EXPLANATION_OF_CHANGES.md) for detailed changelog.

---

**Questions?** Open an [issue](https://github.com/jon-becker/prediction-market-analysis/issues) or check the [documentation](docs/README.md).
