#!/usr/bin/env python3
"""
kmeans_segmentation.py — Day 41 | KMeans Customer Segmentation
===============================================================
Finds natural customer segments using KMeans clustering.
Compares with existing rule-based Bronze/Silver/Gold/Platinum segments.

Run: python kmeans_segmentation.py
"""

from __future__ import annotations

import sys
from pathlib import Path
import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA
from sklearn.metrics import silhouette_score

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-02"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04"))

from db_utils import get_engine, dispose_engine
from logger import get_pipeline_logger

logger = get_pipeline_logger("kmeans")

OUTPUT_DIR = Path(__file__).parent / "output"
OUTPUT_DIR.mkdir(exist_ok=True)


def load_customer_features() -> pd.DataFrame:
    """Load customer analytics data for clustering."""
    engine = get_engine()
    df = pd.read_sql("""
        SELECT
            customer_id,
            segment,
            total_rentals,
            total_spend,
            days_since_last_payment,
            is_active
        FROM analytics_customer_summary
        WHERE total_spend IS NOT NULL
    """, engine)
    dispose_engine()
    logger.info(f"Loaded {len(df)} customers for clustering")
    return df


def prepare_features(df: pd.DataFrame) -> tuple[np.ndarray, StandardScaler]:
    """Scale features for KMeans."""
    feature_cols = ["total_rentals", "total_spend", "days_since_last_payment"]
    X = df[feature_cols].fillna(0).to_numpy()
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)
    return X_scaled, scaler


# ── Elbow method — provided ───────────────────────────────────────────────
def find_optimal_k(X_scaled: np.ndarray, k_range: range = range(2, 9)) -> int:
    """
    Run KMeans for multiple k values.
    Plot inertia (elbow curve) and silhouette scores.
    Return optimal k.
    """
    inertias    = []
    silhouettes = []

    for k in k_range:
        km = KMeans(n_clusters=k, random_state=42, n_init=10)
        km.fit(X_scaled)
        inertias.append(km.inertia_)
        sil = silhouette_score(X_scaled, km.labels_)
        silhouettes.append(sil)
        logger.info(f"k={k}: inertia={km.inertia_:,.1f} silhouette={sil:.4f}")

    # Plot elbow curve
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 4))

    ax1.plot(list(k_range), inertias, "bo-", linewidth=2)
    ax1.set_xlabel("Number of Clusters (k)")
    ax1.set_ylabel("Inertia")
    ax1.set_title("Elbow Curve — Find Optimal k")
    ax1.grid(True, alpha=0.3)

    ax2.plot(list(k_range), silhouettes, "ro-", linewidth=2)
    ax2.set_xlabel("Number of Clusters (k)")
    ax2.set_ylabel("Silhouette Score")
    ax2.set_title("Silhouette Score (higher = better)")
    ax2.grid(True, alpha=0.3)

    fig.tight_layout()
    elbow_path = OUTPUT_DIR / "elbow_curve.png"
    fig.savefig(elbow_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Elbow curve saved → {elbow_path.name}")

    # Best k = highest silhouette score
    best_k = list(k_range)[np.argmax(silhouettes)]
    logger.info(f"Optimal k (best silhouette): {best_k}")
    return best_k


# ── Final clustering — provided ───────────────────────────────────────────
def run_final_clustering(X_scaled: np.ndarray, k: int) -> np.ndarray:
    """Run KMeans with optimal k. Returns cluster labels."""
    km = KMeans(n_clusters=k, random_state=42, n_init=10)
    labels = km.fit_predict(X_scaled)
    logger.info(f"Cluster sizes: {dict(zip(*np.unique(labels, return_counts=True)))}")
    return labels


# ── Visualise clusters with PCA — provided ───────────────────────────────
def visualise_clusters(X_scaled: np.ndarray, labels: np.ndarray) -> None:
    """Reduce to 2D with PCA and plot clusters."""
    pca = PCA(n_components=2)
    X_2d = pca.fit_transform(X_scaled)
    var_explained = pca.explained_variance_ratio_.sum()

    fig, ax = plt.subplots(figsize=(9, 6))
    scatter = ax.scatter(
        X_2d[:, 0], X_2d[:, 1],
        c=labels, cmap="tab10", alpha=0.6, s=30,
    )
    plt.colorbar(scatter, ax=ax, label="Cluster")
    ax.set_xlabel(f"PC1 ({pca.explained_variance_ratio_[0]:.1%} variance)")
    ax.set_ylabel(f"PC2 ({pca.explained_variance_ratio_[1]:.1%} variance)")
    ax.set_title(f"Customer Clusters (PCA 2D, {var_explained:.1%} variance explained)")
    ax.grid(True, alpha=0.3)

    fig.tight_layout()
    cluster_path = OUTPUT_DIR / "cluster_scatter.png"
    fig.savefig(cluster_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"Cluster scatter saved → {cluster_path.name}")


# ── Q: Cluster profiling — WRITE THIS YOURSELF ───────────────────────────
def profile_clusters(df: pd.DataFrame, labels: np.ndarray) -> pd.DataFrame:
    """
    Q — YOUR TASK:
    Add cluster labels to DataFrame and build per-cluster profile.

    HINTS:
    Step 1: Add cluster column
        df = df.copy()
        df["cluster"] = labels

    Step 2: Group by cluster, compute stats
        profile = df.groupby("cluster").agg(
            customer_count=("customer_id", "count"),
            avg_spend=("total_spend", "mean"),
            avg_rentals=("total_rentals", "mean"),
            avg_days_since_payment=("days_since_last_payment", "mean"),
            active_rate=("is_active", "mean"),
            pct_platinum=("segment",
                         lambda x: (x == "Platinum").mean()),
        ).round(2).reset_index()

    Step 3: Add cluster label (name each cluster based on profile)
        def name_cluster(row):
            if row["avg_spend"] > 150:   return "High Value"
            elif row["avg_spend"] > 100: return "Medium Value"
            elif row["active_rate"] < 0.8: return "At Risk"
            else:                        return "Standard"

        profile["cluster_name"] = profile.apply(name_cluster, axis=1)

    Step 4: Log and save
        logger.info(f"\n── Cluster Profiles ──────────────────────")
        logger.info(f"\n{profile.to_string(index=False)}")
        profile.to_csv(OUTPUT_DIR / "cluster_profiles.csv", index=False)

    Step 5: Return (df with cluster column, profile DataFrame)
        return df, profile
    """
    # YOUR CODE HERE
    df = df.copy()
    df["cluster"] = labels
    profile = df.groupby("cluster").agg(
        customer_count=("customer_id", "count"),
        avg_spend=("total_spend", "mean"),
        avg_rentals=("total_rentals", "mean"),
        avg_days_since_payment=("days_since_last_payment", "mean"),
        active_rate=("is_active", "mean"),
        pct_platinum=("segment",
                     lambda x: (x == "Platinum").mean()),
    ).round(2).reset_index()
    def name_cluster(row):
        if row["avg_spend"] > 150:   return "High Value"
        elif row["avg_spend"] > 100: return "Medium Value"
        elif row["active_rate"] < 0.8: return "At Risk"
        else:                        return "Standard"

    profile["cluster_name"] = profile.apply(name_cluster, axis=1)
    logger.info(f"\n── Cluster Profiles ──────────────────────")
    logger.info(f"\n{profile.to_string(index=False)}")
    profile.to_csv(OUTPUT_DIR / "cluster_profiles.csv", index=False)
    return df, profile


# ── Write to DB — provided ───────────────────────────────────────────────

def write_clusters_to_db(df: pd.DataFrame) -> None:
    """Write cluster assignments back to analytics_customer_airflow."""
    engine = get_engine()
    # Add cluster columns to existing table
    updates = df[["customer_id", "cluster"]].copy()
    updates.to_sql(
        "analytics_customer_clusters", engine,
        if_exists="replace", index=False, method="multi",
    )
    dispose_engine()
    logger.info(f"Cluster assignments written → analytics_customer_clusters ({len(updates)} rows)")


def main() -> None:
    logger.info("=" * 52)
    logger.info("KMeans Customer Segmentation — Day 41")
    logger.info("=" * 52)

    df = load_customer_features()
    X_scaled, scaler = prepare_features(df)

    # Find optimal k
    optimal_k = find_optimal_k(X_scaled)

    # Cluster
    labels = run_final_clustering(X_scaled, optimal_k)

    # Visualise
    visualise_clusters(X_scaled, labels)

    # Profile — your implementation
    df_clustered, profile = profile_clusters(df, labels)

    # Write to DB
    write_clusters_to_db(df_clustered)

    logger.info(f"\n✅ Segmentation complete: {optimal_k} clusters found")
    logger.info(f"Output: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()