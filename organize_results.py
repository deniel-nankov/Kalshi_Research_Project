#!/usr/bin/env python3
"""Organize analysis results into separate folders."""

import shutil
from pathlib import Path

def organize_results():
    """Move each analysis's files into its own folder."""
    results_dir = Path("analysis_results")
    all_files = list(results_dir.glob("*.*"))
    
    # Get unique analysis names (without extensions)
    analysis_names = set()
    for file in all_files:
        if file.suffix in ['.png', '.pdf', '.csv', '.json']:
            analysis_names.add(file.stem)
    
    # Remove the meta file from list
    if 'linked_markets_trades' in analysis_names:
        analysis_names.remove('linked_markets_trades')
    
    print(f"Found {len(analysis_names)} analyses to organize\n")
    
    # Create folders and move files
    for analysis_name in sorted(analysis_names):
        # Create folder
        folder = results_dir / analysis_name
        folder.mkdir(exist_ok=True)
        
        # Move all related files
        moved = []
        for ext in ['.png', '.pdf', '.csv', '.json']:
            src = results_dir / f"{analysis_name}{ext}"
            if src.exists():
                dst = folder / f"{analysis_name}{ext}"
                shutil.move(str(src), str(dst))
                moved.append(ext)
        
        print(f"✓ {analysis_name}/  ({', '.join(moved)})")
    
    print(f"\n✅ Organized {len(analysis_names)} analyses into separate folders")
    print(f"✅ Kept linked_markets_trades.csv in root")

if __name__ == "__main__":
    organize_results()
