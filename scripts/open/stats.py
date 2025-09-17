#!/usr/bin/env python3
"""Analyze Parquet file statistics using memory-efficient scanning."""

import argparse
from pathlib import Path
import polars as pl

def analyze_parquet_stats(parquet_path: Path) -> dict:
    """Analyze basic statistics from a Parquet file using lazy evaluation.

    Args:
        parquet_path: Path to the Parquet file

    Returns:
        Dictionary containing statistics
    """
    if not parquet_path.exists():
        raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

    # Use lazy frame for memory efficiency
    lazy_df = pl.scan_parquet(parquet_path)

    # Get basic stats with a single query
    stats = lazy_df.select([
        pl.len().alias("total_logs"),
        pl.col("timeStamp").cast(pl.Int64).max().alias("max_timestamp"),
        pl.col("timeStamp").cast(pl.Int64).min().alias("min_timestamp"),
        pl.col("blockNumber").cast(pl.Int64).max().alias("max_block"),
        pl.col("blockNumber").cast(pl.Int64).min().alias("min_block"),
        pl.col("contract_address").n_unique().alias("unique_contracts"),
        pl.col("protocol").first().alias("protocol")
    ]).collect()

    # Convert to dict for easier access
    result = stats.to_dicts()[0]

    # Get file size
    file_size_mb = parquet_path.stat().st_size / (1024 * 1024)
    result["file_size_mb"] = round(file_size_mb, 2)
    result["file_path"] = str(parquet_path)

    return result

def format_timestamp(timestamp) -> str:
    """Convert Unix timestamp to readable format."""
    if timestamp is None:
        return "N/A"
    from datetime import datetime
    return datetime.fromtimestamp(timestamp).strftime("%Y-%m-%d %H:%M:%S")

def print_stats(stats: dict):
    """Print formatted statistics."""
    print("=" * 60)
    print("📊 PARQUET FILE STATISTICS")
    print("=" * 60)
    print(f"📁 File: {stats['file_path']}")
    print(f"📏 Size: {stats['file_size_mb']:.2f} MB")
    print(f"🏷️  Protocol: {stats['protocol']}")
    print(f"📊 Total Logs: {stats['total_logs']:,}")
    print(f"🏠 Unique Contracts: {stats['unique_contracts']}")
    print()
    print("🕐 Time Range:")
    print(f"   Min: {format_timestamp(stats['min_timestamp'])} (timestamp: {stats['min_timestamp'] or 'N/A'})")
    print(f"   Max: {format_timestamp(stats['max_timestamp'])} (timestamp: {stats['max_timestamp'] or 'N/A'})")
    print()
    print("📦 Block Range:")
    min_block = stats['min_block']
    max_block = stats['max_block']

    if min_block is not None and max_block is not None:
        print(f"   Min Block: {min_block:,}")
        print(f"   Max Block: {max_block:,}")
        print(f"   Block Span: {max_block - min_block:,}")
    else:
        print(f"   Min Block: {min_block or 'N/A'}")
        print(f"   Max Block: {max_block or 'N/A'}")
        print("   Block Span: N/A")
    print("=" * 60)

def main():
    parser = argparse.ArgumentParser(description="Analyze Parquet file statistics")
    parser.add_argument("file_path", nargs="?",
                       help="Path to Parquet file (default: data/etherscan_raw/protocol=misc/logs.parquet)")
    parser.add_argument("--json", action="store_true",
                       help="Output as JSON instead of formatted text")

    args = parser.parse_args()

    # Default to the most recent extraction
    if args.file_path is None:
        default_path = Path("data/etherscan_raw/protocol=misc/logs.parquet")
        parquet_path = default_path
    else:
        parquet_path = Path(args.file_path)

    try:
        stats = analyze_parquet_stats(parquet_path)

        if args.json:
            import json
            print(json.dumps(stats, indent=2))
        else:
            print_stats(stats)

    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

    return 0

if __name__ == "__main__":
    exit(main())