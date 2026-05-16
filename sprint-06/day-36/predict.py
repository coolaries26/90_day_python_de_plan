"""
predict.py — Day 36 | Load Model + Predict Churn
=================================================
YOUR TASK: Load saved model and predict churn for all customers.

HINTS:
import joblib, json
from pathlib import Path

MODEL_DIR = Path(__file__).parent / "models"

Step 1: Load model metadata
    with open(MODEL_DIR / "model_metadata.json") as f:
        meta = json.load(f)

Step 2: Load model + scaler
    model  = joblib.load(MODEL_DIR / meta["model_file"])
    scaler = joblib.load(MODEL_DIR / meta["scaler_file"])

Step 3: Load features (reuse feature_engineering.py)
    from feature_engineering import main as get_features
    X, y = get_features()

Step 4: Scale and predict
    X_scaled = scaler.transform(X)
    predictions = model.predict(X_scaled)
    probabilities = model.predict_proba(X_scaled)[:, 1]  # prob of being active

Step 5: Build results DataFrame
    results = pd.DataFrame({
        "predicted_active":   predictions,
        "churn_probability":  (1 - probabilities).round(4),  # prob of churning
        "actual_active":      y.values,
        "correct":            (predictions == y.values).astype(int),
    })

Step 6: Print summary
    print(f"Predicted active:  {predictions.sum()}")
    print(f"Predicted churned: {(predictions == 0).sum()}")
    print(f"Overall accuracy:  {results['correct'].mean():.1%}")

Step 7: Save to output/predictions.csv
    results.to_csv(Path(__file__).parent / "output" / "predictions.csv", index=False)
    print("Saved → predictions.csv")
"""
# YOUR CODE HERE
import json
import sys
import joblib
import pandas as pd
from pathlib import Path
from feature_engineering import main as get_features
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent / "sprint-01" / "day-04")) # for logger

from logger import get_pipeline_logger 
logger = get_pipeline_logger("predict") # for consistent logging across pipeline steps

MODEL_DIR = Path(__file__).parent / "models"    # directory for loading trained model and scaler

def predict() -> pd.DataFrame:
    print("=" * 52)
    print("Churn Prediction Results — Day 36")
    print("=" * 52)
    # Step 1: Load model metadata
    with open(MODEL_DIR / "model_metadata.json") as f:
        meta = json.load(f) 
    logger.info(f"Loaded model metadata: {meta['model_type']} trained at {meta['trained_at']} with features: {meta['feature_names']}") 
    
    # Step 2: Load model + scaler
    model  = joblib.load(MODEL_DIR / meta["model_file"])
    scaler = joblib.load(MODEL_DIR / meta["scaler_file"])
    logger.info(f"Loaded model → {meta['model_file']}")
    logger.info(f"Loaded scaler → {meta['scaler_file']}")
    
    # Step 3: Load features
    X, y = get_features()
    logger.info(f"Loaded features for {len(X)} customers")
    
    # Step 4: Scale and predict
    X_scaled = scaler.transform(X)
    predictions = model.predict(X_scaled)
    probabilities = model.predict_proba(X_scaled)[:, 1]  # prob of being active 
    logger.info(f"Made predictions for {len(predictions)} customers")
    
    # Step 5: Build results DataFrame
    results = pd.DataFrame({
        "predicted_active":   predictions,
        "churn_probability":  (1 - probabilities).round(4),  # prob of churning
        "actual_active":      y.values, 
        "correct":            (predictions == y.values).astype(int),
    })
    logger.info(f"Built results DataFrame with {len(results)} rows")
    # Step 6: Print summary
    logger.info(f"Predicted active:  {predictions.sum()}")
    logger.info(f"Predicted churned: {(predictions == 0).sum()}")
    logger.info(f"Overall accuracy:  {results['correct'].mean():.1%}")
    # Step 7: Save to output/predictions.csv
    OUTPUT_DIR = Path(__file__).parent / "output"
    OUTPUT_DIR.mkdir(exist_ok=True)
    results.to_csv(OUTPUT_DIR / "predictions.csv", index=False)
    logger.info(f"Saved predictions to {OUTPUT_DIR / 'predictions.csv'}")
    return results

if __name__ == "__main__":
    results = predict()
    print("=" * 52)
    print("=" * 52)
    print("Churn Prediction Results — Day 36")
    print("=" * 52)
    print(results.head())
    print("\n── Prediction Summary ────────────────")
    print(f"Total customers:    {len(results)}")
    print(f"Predicted active:  {results['predicted_active'].sum()}")
    print(f"Predicted churned: {(results['predicted_active'] == 0).sum()}")
    print(f"Overall accuracy:  {results['correct'].mean():.1%}")
    print(f"Average churn probability: {results['churn_probability'].mean():.2%}")
    print(f"Average churn probability (predicted churned): {results.loc[results['predicted_active'] == 0, 'churn_probability'].mean():.2%}")    
    print(f"Average churn probability (predicted active): {results.loc[results['predicted_active'] == 1, 'churn_probability'].mean():.2%}") 
    print(f"Actual active:     {results['actual_active'].sum()}")
    print(f"Actual churned:    {(results['actual_active'] == 0).sum()}")
    print(f"Accuracy for predicted active:  {(results.loc[results['predicted_active'] == 1, 'correct'].mean()):.1%}")
    print(f"Accuracy for predicted churned: {(results.loc[results['predicted_active'] == 0, 'correct'].mean()):.1%}")
