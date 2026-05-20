# 📅 DAY 41 — Sprint 06 | KMeans Customer Segmentation
## Data-Driven Clusters, Elbow Method, Segment Profiles, Streamlit Clusters Page

---

## 🔁 RETROSPECTIVE — Day 40

### Instructor Assessment
| Item | Result | Note |
|------|--------|------|
| drift_detector.py standalone | ✅ Pass | All 4 thresholds correct |
| Task 5 in dag_ml_retrain | ✅ Pass | |
| ml_drift_log table | ✅ Pass | Correct schema |
| Streamlit drift alert | ✅ Pass | Simulated drift visible |

### Pre-Start
```bash
cd C:\90_day_python_de_plan
git checkout develop
git pull origin develop
git checkout -b sprint-06/day-41-clustering
```

---

## 🗂️ JIRA CARD

| Field           | Value                                                        |
|-----------------|--------------------------------------------------------------|
| Epic            | EP-09: ML Foundations & Scikit-learn                         |
| Story           | ST-41: KMeans Customer Segmentation                          |
| Task ID         | TASK-041                                                     |
| Sprint          | Sprint 06 (Days 36–42)                                       |
| Story Points    | 3                                                            |
| Priority        | HIGH                                                         |
| Labels          | kmeans, clustering, segmentation, streamlit, day-41          |
| Acceptance Criteria | KMeans finds optimal k via elbow; clusters profiled; assignments written to DB; Streamlit clusters page |

---

## 📚 BACKGROUND

### Rule-Based vs Data-Driven Segmentation

```
Current segments (Day 14 — rule-based):
  Bronze:   total_spend <= $50
  Silver:   total_spend <= $100
  Gold:     total_spend <= $150
  Platinum: total_spend > $150

Problem: arbitrary thresholds, only one dimension (spend)
         customers with same spend but very different behaviour
         get the same segment label

KMeans (data-driven):
  Finds natural groupings in n-dimensional feature space
  No thresholds — the algorithm discovers the boundaries
  Uses multiple features simultaneously: spend + rentals + recency + ...
```

### KMeans Algorithm

```
1. Choose k (number of clusters)
2. Randomly place k centroids in feature space
3. Assign each point to nearest centroid
4. Move centroids to mean of assigned points
5. Repeat 3-4 until centroids stop moving

Elbow method — choose optimal k:
  Run KMeans for k=1,2,3,...,10
  Plot inertia (sum of squared distances to nearest centroid)
  Find the "elbow" where inertia stops dropping sharply
  That k is the optimal number of clusters
```

---

## 🎯 OBJECTIVES

1. Run KMeans with elbow method to find optimal k
2. Profile each cluster (mean features per cluster)
3. Visualise clusters with 2D scatter (PCA reduction)
4. Write cluster assignments to `analytics_customer_airflow`
5. Add "Customer Clusters" page to Streamlit
6. Push clean `[DAY-041][S06]`

---

## ⏱️ TIME BUDGET (2 hrs)

| Block | Duration | Activity                                          |
|-------|----------|----------------------------------------------------|
| A     | 10 min   | Branch setup                                       |
| B     | 40 min   | `kmeans_segmentation.py` — full clustering         |
| C     | 30 min   | `pages/clusters.py` — Streamlit clusters page      |
| D     | 20 min   | Write to DB + verify                               |
| E     | 20 min   | Git push                                           |

---

## 📝 EXERCISES

---

### EXERCISE 1 — kmeans_segmentation.py (Block B)
**[Elbow method + clustering provided. Cluster profiling — write yourself]**

Create `sprint-06/day-41/kmeans_segmentation.py`:

```python
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
            value_segment,
            total_rentals,
            total_spend,
            days_since_last_payment,
            is_active
        FROM analytics_customer_airflow
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
            pct_platinum=("value_segment",
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
    raise NotImplementedError("Implement profile_clusters")


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
```

---

### EXERCISE 2 — pages/clusters.py (Block C)
**[Write yourself — follows same pattern as customers.py]**

Create `sprint-05/day-31/pages/clusters.py`:

```python
"""
clusters.py — Customer Clusters Streamlit Page
===============================================
YOUR TASK: Build the customer clusters page.

Requirements:
1. Page title: "🔵 Customer Clusters"
2. Load cluster data:
   cluster_df = pd.read_sql(
       "SELECT c.*, a.total_spend, a.total_rentals, a.value_segment
        FROM analytics_customer_clusters c
        JOIN analytics_customer_airflow a USING (customer_id)",
       engine
   )
3. Show metric: Number of clusters (cluster_df["cluster"].nunique())
4. Show cluster size bar chart:
   px.bar(cluster_df.groupby("cluster").size().reset_index(name="count"),
          x="cluster", y="count", title="Customers per Cluster")
5. Show scatter: total_spend vs total_rentals, colored by cluster
6. Show cluster profiles table (load from cluster_profiles.csv)
7. Add to app.py sidebar as "🔵 Clusters"

HINTS for loading profile CSV:
  profile_path = Path(__file__).resolve().parent.parent.parent
      / "sprint-06" / "day-41" / "output" / "cluster_profiles.csv"
  if profile_path.exists():
      st.dataframe(pd.read_csv(profile_path))
"""
# YOUR CODE HERE
```

---

### EXERCISE 3 — Git Push

```bash
python scripts/daily_commit.py --day 41 --sprint 6 ^
    --message "KMeans clustering: elbow method, cluster profiles, DB write, Streamlit clusters page" ^
    --merge
```

---

## ✅ DAY 41 COMPLETION CHECKLIST

| # | Task                                                                     | Done? |
|---|--------------------------------------------------------------------------|-------|
| 1 | `kmeans_segmentation.py` runs — elbow_curve.png saved                   | [ ]   |
| 2 | Optimal k identified via silhouette score                                | [ ]   |
| 3 | `cluster_scatter.png` saved — PCA 2D visualisation                      | [ ]   |
| 4 | **`profile_clusters()` written — cluster_profiles.csv saved**           | [ ]   |
| 5 | `analytics_customer_clusters` table has 599 rows                         | [ ]   |
| 6 | **`pages/clusters.py` written — cluster bar + scatter + profile table** | [ ]   |
| 7 | Clusters page accessible from Streamlit sidebar                          | [ ]   |
| 8 | One clean `[DAY-041][S06]` commit via `daily_commit.py --merge`          | [ ]   |

---

## 🔍 SELF-CHECK

```bash
ls sprint-06/day-41/output/
# elbow_curve.png   (>20KB)
# cluster_scatter.png (>30KB)
# cluster_profiles.csv

python -c "
import sys; sys.path.insert(0, 'sprint-01/day-02')
from db_utils import execute_scalar, close_pool
print('Cluster assignments:', execute_scalar('SELECT COUNT(*) FROM analytics_customer_clusters'))
print('Distinct clusters:',   execute_scalar('SELECT COUNT(DISTINCT cluster) FROM analytics_customer_clusters'))
close_pool()
"
```

---

## 🔜 PREVIEW: DAY 42 — Sprint 06 Test

**Sprint 06 final test** — 4 ML tasks covering everything from Days 36–41.
Close sprint with `sprint-06-complete` tag.
Sprint 07 begins Day 43: Capstone project design.

---

*Day 41 | Sprint 06 | EP-09 | TASK-041*
