"""Data processing pipeline tutorial for SLURM SDK."""

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from typing import Optional

import numpy as np

from slurm.cluster import Cluster
from slurm.decorators import task
from slurm.runtime import JobContext


def simple_tokenize(text: str) -> list[str]:
    """Simple word tokenization."""
    # Convert to lowercase and split on non-alphanumeric
    words = re.findall(r"\b\w+\b", text.lower())
    return words


def compute_word_frequencies(tokens: list[str]) -> dict[str, int]:
    """Compute word frequency counts."""
    return dict(Counter(tokens))


def compute_tfidf_features(tokens: list[str], max_features: int = 100) -> np.ndarray:
    """Simple TF-IDF feature extraction (single document)."""
    freq = compute_word_frequencies(tokens)

    # Get top words by frequency
    top_words = sorted(freq.items(), key=lambda x: x[1], reverse=True)[:max_features]

    # Create feature vector (normalized frequencies)
    total = sum(freq.values())
    features = np.array([count / total for word, count in top_words])

    return features


@task(time="00:30:00", mem="4G", cpus_per_task=4)
def tokenize_text(
    input_path: str,
    *,
    job: Optional[JobContext] = None,
) -> dict:
    """Tokenize and clean input text file.

    Args:
        input_path: Path to input text file
        job: Optional JobContext (injected when running on cluster)

    Returns:
        dict with tokens_path and statistics
    """
    print(f"Processing file: {input_path}")

    # Read input
    with open(input_path) as f:
        text = f.read()

    # Tokenize
    tokens = simple_tokenize(text)

    # Compute statistics
    stats = {
        "num_tokens": len(tokens),
        "num_unique": len(set(tokens)),
        "sample_tokens": tokens[:10],
    }

    print(f"Tokenized {stats['num_tokens']} tokens ({stats['num_unique']} unique)")

    # Determine output path
    if job and job.output_dir:
        output_dir = job.output_dir
        print(f"Job output directory: {output_dir}")
    else:
        output_dir = Path("./output")
        output_dir.mkdir(exist_ok=True)

    # Save tokens
    tokens_path = output_dir / "tokens.json"
    with open(tokens_path, "w") as f:
        json.dump(tokens, f)

    # Save statistics
    stats_path = output_dir / "tokenize_stats.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    result = {
        "tokens_path": str(tokens_path),
        "stats_path": str(stats_path),
        "statistics": stats,
    }

    print(f"Saved tokens to {tokens_path}")
    return result


@task(time="01:00:00", mem="8G", cpus_per_task=8)
def extract_features(
    tokens_path: str,
    max_features: int = 100,
    *,
    job: Optional[JobContext] = None,
) -> dict:
    """Extract TF-IDF features from tokenized data.

    Args:
        tokens_path: Path to tokens JSON file
        max_features: Maximum number of features to extract
        job: Optional JobContext

    Returns:
        dict with features_path and statistics
    """
    print(f"Loading tokens from: {tokens_path}")

    # Load tokens
    with open(tokens_path) as f:
        tokens = json.load(f)

    # Extract features
    features = compute_tfidf_features(tokens, max_features)

    stats = {
        "feature_dim": len(features),
        "mean": float(features.mean()),
        "std": float(features.std()),
        "min": float(features.min()),
        "max": float(features.max()),
    }

    print(f"Extracted {stats['feature_dim']} features")

    # Determine output path
    if job and job.output_dir:
        output_dir = job.output_dir
    else:
        output_dir = Path("./output")
        output_dir.mkdir(exist_ok=True)

    # Save features
    features_path = output_dir / "features.npy"
    np.save(features_path, features)

    # Save statistics
    stats_path = output_dir / "features_stats.json"
    with open(stats_path, "w") as f:
        json.dump(stats, f, indent=2)

    result = {
        "features_path": str(features_path),
        "stats_path": str(stats_path),
        "statistics": stats,
    }

    print(f"Saved features to {features_path}")
    return result


@task(time="00:15:00", mem="2G")
def compute_statistics(
    features_path: str,
    *,
    job: Optional[JobContext] = None,
) -> dict:
    """Compute detailed statistics from extracted features.

    Args:
        features_path: Path to features NPY file
        job: Optional JobContext

    Returns:
        dict with statistics
    """
    print(f"Loading features from: {features_path}")

    # Load features
    features = np.load(features_path)

    # Compute statistics
    stats = {
        "shape": features.shape,
        "dtype": str(features.dtype),
        "mean": float(features.mean()),
        "median": float(np.median(features)),
        "std": float(features.std()),
        "min": float(features.min()),
        "max": float(features.max()),
        "percentiles": {
            "25": float(np.percentile(features, 25)),
            "50": float(np.percentile(features, 50)),
            "75": float(np.percentile(features, 75)),
            "95": float(np.percentile(features, 95)),
        },
    }

    print("Computed statistics:")
    print(json.dumps(stats, indent=2))

    # Save statistics if on cluster
    if job and job.output_dir:
        stats_path = job.output_dir / "final_stats.json"
        with open(stats_path, "w") as f:
            json.dump(stats, f, indent=2)
        print(f"Saved statistics to {stats_path}")

    return stats


def run_local_pipeline(input_path: str, output_dir: str):
    """Run full pipeline locally (single process)."""
    print("=== Running local pipeline ===\n")

    # Stage 1: Tokenization
    print("Stage 1: Tokenization")
    result1 = tokenize_text(input_path)
    tokens_path = result1["tokens_path"]
    print(f"✓ Tokens: {result1['statistics']['num_tokens']}\n")

    # Stage 2: Feature extraction
    print("Stage 2: Feature Extraction")
    result2 = extract_features(tokens_path)
    features_path = result2["features_path"]
    print(f"✓ Features: {result2['statistics']['feature_dim']}\n")

    # Stage 3: Statistics
    print("Stage 3: Statistics")
    stats = compute_statistics(features_path)
    print(f"✓ Mean: {stats['mean']:.4f}, Std: {stats['std']:.4f}\n")

    print("=== Pipeline complete ===")
    print(f"Output directory: {output_dir}")

    return stats


def run_cluster_pipeline(input_path: str, cluster: Cluster):
    """Run full pipeline on SLURM cluster (chained jobs)."""
    print("=== Submitting pipeline to cluster ===\n")

    # Stage 1: Tokenization
    print("Stage 1: Submitting tokenization job...")
    job1 = cluster.submit(tokenize_text)(input_path)
    print(f"Job ID: {job1.id}")

    print("Waiting for tokenization...")
    success1 = job1.wait(timeout=1800, poll_interval=10)

    if not success1:
        print("Tokenization job failed!")
        print(f"Status: {job1.get_status()}")
        return None

    result1 = job1.get_result()
    tokens_path = result1["tokens_path"]
    print(f"✓ Tokenization complete: {result1['statistics']['num_tokens']} tokens\n")

    # Stage 2: Feature extraction
    print("Stage 2: Submitting feature extraction job...")
    job2 = cluster.submit(extract_features)(tokens_path)
    print(f"Job ID: {job2.id}")

    print("Waiting for feature extraction...")
    success2 = job2.wait(timeout=3600, poll_interval=10)

    if not success2:
        print("Feature extraction job failed!")
        print(f"Status: {job2.get_status()}")
        return None

    result2 = job2.get_result()
    features_path = result2["features_path"]
    print(
        f"✓ Feature extraction complete: {result2['statistics']['feature_dim']} features\n"
    )

    # Stage 3: Statistics
    print("Stage 3: Submitting statistics job...")
    job3 = cluster.submit(compute_statistics)(features_path)
    print(f"Job ID: {job3.id}")

    print("Waiting for statistics...")
    success3 = job3.wait(timeout=900, poll_interval=10)

    if not success3:
        print("Statistics job failed!")
        print(f"Status: {job3.get_status()}")
        return None

    stats = job3.get_result()
    print("✓ Statistics complete\n")

    print("=== Pipeline complete ===")
    print("Statistics:")
    print(json.dumps(stats, indent=2))

    return stats


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Data processing pipeline")
    parser.add_argument("--input", required=True, help="Input text file")
    parser.add_argument(
        "--output", default="./output", help="Output directory (local mode)"
    )
    parser.add_argument(
        "--pipeline", action="store_true", help="Run full pipeline locally"
    )
    parser.add_argument(
        "--submit", action="store_true", help="Submit single job to cluster"
    )
    parser.add_argument(
        "--submit-pipeline", action="store_true", help="Submit full pipeline to cluster"
    )
    parser.add_argument("--env", default="local", help="Slurmfile environment")
    parser.add_argument(
        "--slurmfile", default="Slurmfile.toml", help="Path to Slurmfile"
    )
    parser.add_argument(
        "--stage", choices=["tokenize", "features", "stats"], help="Run single stage"
    )

    args = parser.parse_args()

    if args.pipeline:
        # Run full pipeline locally
        run_local_pipeline(args.input, args.output)

    elif args.submit_pipeline:
        # Submit full pipeline to cluster
        cluster = Cluster.from_env(args.slurmfile, env=args.env)
        run_cluster_pipeline(args.input, cluster)

    elif args.submit:
        # Submit single job
        cluster = Cluster.from_env(args.slurmfile, env=args.env)

        if args.stage == "tokenize" or not args.stage:
            print("Submitting tokenization job...")
            job = cluster.submit(tokenize_text)(args.input)
            print(f"Job ID: {job.id}")
            print("Waiting...")
            if job.wait():
                result = job.get_result()
                print(f"Complete: {result}")

    else:
        # Run single stage locally
        if args.stage == "tokenize" or not args.stage:
            result = tokenize_text(args.input)
            print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
