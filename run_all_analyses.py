"""Convert historical CSV to Parquet and run all analyses."""
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import subprocess

print("="*70)
print("PREPARING HISTORICAL DATA FOR COMPREHENSIVE ANALYSIS")
print("="*70)

# Load CSV files
markets_df = pd.read_csv('data/kalshi/raw/historical_markets.csv')
trades_df = pd.read_csv('data/kalshi/raw/historical_trades.csv')

print(f"\n✓ Loaded {len(markets_df)} markets, {len(trades_df):,} trades")
print(f"  Markets with results: {markets_df['result'].notna().sum()}")
print(f"  Result distribution: {markets_df['result'].value_counts().to_dict()}")
print(f"  Total volume: ${trades_df['count'].sum():,.0f}")

# Create Parquet files
os.makedirs('data/kalshi/markets', exist_ok=True)
os.makedirs('data/kalshi/trades', exist_ok=True)

pq.write_table(pa.Table.from_pandas(markets_df), 'data/kalshi/markets/historical_001.parquet')
pq.write_table(pa.Table.from_pandas(trades_df), 'data/kalshi/trades/historical_001.parquet')

print(f"✓ Created Parquet files in data/kalshi/")

# Create output folder for results
os.makedirs('analysis_results', exist_ok=True)

print("\n" + "="*70)
print("RUNNING ALL 23 BUILT-IN ANALYSES")
print("="*70)

analyses = [
    'meta_stats',
    'market_types',
    'trade_size_by_role',
    'maker_vs_taker_returns',
    'maker_win_rate_by_direction',
    'maker_returns_by_direction',
    'mispricing_by_price',
    'win_rate_by_price',
    'yes_vs_no_by_price',
    'volume_over_time',
    'returns_by_hour',
    'vwap_by_hour',
    'maker_taker_gap_over_time',
    'maker_taker_returns_by_category',
    'longshot_volume_share_over_time',
    'kalshi_calibration_deviation_over_time',
    'statistical_tests',
    'ev_yes_vs_no',
    'win_rate_by_trade_size',
]

successful = []
failed = []

for i, analysis in enumerate(analyses):
    print(f"\n[{i+1}/{len(analyses)}] {analysis}...", end='', flush=True)
    
    try:
        result = subprocess.run(
            ['uv', 'run', 'python', 'main.py', 'analyze', analysis],
            capture_output=True,
            text=True,
            timeout=60
        )
        
        if result.returncode == 0:
            print(" ✓")
            successful.append(analysis)
        else:
            print(f" ✗")
            failed.append((analysis, result.stderr[:200] if result.stderr else "Unknown error"))
    except subprocess.TimeoutExpired:
        print(" ✗ (timeout)")
        failed.append((analysis, "Timeout after 60s"))
    except Exception as e:
        print(f" ✗ ({str(e)[:50]})")
        failed.append((analysis, str(e)[:200]))

# Move output files to analysis_results folder
print(f"\n" + "="*70)
print("ORGANIZING RESULTS")
print("="*70)

for ext in ['png', 'pdf', 'csv', 'json']:
    files = subprocess.run(['find', 'output', '-name', f'*.{ext}', '-type', 'f'], 
                          capture_output=True, text=True).stdout.strip().split('\n')
    for file in files:
        if file and os.path.exists(file):
            basename = os.path.basename(file)
            subprocess.run(['cp', file, f'analysis_results/{basename}'], check=False)

print(f"✓ Copied outputs to analysis_results/")

# Summary
print("\n" + "="*70)
print("ANALYSIS SUMMARY")
print("="*70)

print(f"\n✅ SUCCESSFUL: {len(successful)}/{len(analyses)}")
for name in successful:
    print(f"   ✓ {name}")

if failed:
    print(f"\n❌ FAILED: {len(failed)}/{len(analyses)}")
    for name, error in failed:
        print(f"   ✗ {name}: {error[:80]}")

# Count output files
result_files = subprocess.run(['ls', 'analysis_results'], capture_output=True, text=True)
file_list = result_files.stdout.split('\n')
file_count = len([f for f in file_list if f.strip()])
png_count = len([f for f in file_list if '.png' in f])
pdf_count = len([f for f in file_list if '.pdf' in f])
csv_count = len([f for f in file_list if '.csv' in f])
json_count = len([f for f in file_list if '.json' in f])

print(f"\n📊 OUTPUT FILES: {file_count} files in analysis_results/")
print(f"   Charts (PNG): {png_count}")
print(f"   PDFs: {pdf_count}")
print(f"   Data (CSV): {csv_count}")
print(f"   JSON: {json_count}")

print("\n" + "="*70)
print("✅ COMPLETE - Results ready in analysis_results/")
print("="*70)
