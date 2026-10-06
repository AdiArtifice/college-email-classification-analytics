"""
MLOps Drift & Model Observability Engine.
Monitors concept drift (shifts in email category distributions over academic terms)
and data drift (confidence degradation and input vocabulary shifts).
"""

import os
import json
import pandas as pd
from typing import Dict, Any, Tuple

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
FEATURES_PARQUET = os.path.join(PROJECT_ROOT, 'data', 'output', 'features', 'emails.parquet')


def analyze_drift(parquet_path: str = FEATURES_PARQUET) -> Dict[str, Any]:
    """
    Compute data and concept drift metrics from the processed emails Parquet file.
    """
    if not os.path.exists(parquet_path):
        return {
            "status": "error",
            "message": f"Features dataset not found at {parquet_path}"
        }

    try:
        df = pd.read_parquet(
            parquet_path,
            columns=['date_utc', 'category', 'confidence', 'prediction_method', 'campus_type']
        )
    except Exception as e:
        return {"status": "error", "message": str(e)}

    # Ensure datetime
    df['date'] = pd.to_datetime(df['date_utc'], errors='coerce', utc=True)
    df = df.dropna(subset=['date'])
    df['year_month'] = df['date'].dt.strftime('%Y-%m')

    # Filter out empty or out-of-range dates
    df = df[df['year_month'] >= '2023-08']

    # 1. Monthly Category Distribution (Concept Drift across semesters)
    monthly_cat = df.groupby(['year_month', 'category']).size().unstack(fill_value=0)
    monthly_cat_pct = monthly_cat.div(monthly_cat.sum(axis=1), axis=0) * 100

    # 2. Average Model Confidence by Month
    monthly_conf = df.groupby('year_month')['confidence'].mean().round(4).to_dict()

    # 3. Prediction Method Ratio by Month (% Rule vs % MLlib)
    monthly_method = df.groupby(['year_month', 'prediction_method']).size().unstack(fill_value=0)
    monthly_method_pct = monthly_method.div(monthly_method.sum(axis=1), axis=0) * 100

    # 4. Overall Health Metrics
    avg_confidence = float(df['confidence'].mean())
    low_confidence_count = int((df['confidence'] < 0.80).sum())
    total_records = len(df)
    low_confidence_pct = round((low_confidence_count / total_records) * 100, 2)

    # Calculate month-over-month category variance (stability indicator)
    cat_shifts = monthly_cat_pct.diff().abs().mean().to_dict()

    return {
        "status": "healthy",
        "total_records": total_records,
        "overall_avg_confidence": round(avg_confidence, 4),
        "low_confidence_records": low_confidence_count,
        "low_confidence_percentage": low_confidence_pct,
        "monthly_confidence": monthly_conf,
        "monthly_category_pct": monthly_cat_pct.reset_index().to_dict(orient='records'),
        "monthly_method_pct": monthly_method_pct.reset_index().to_dict(orient='records'),
        "category_instability_index": {k: round(v, 2) for k, v in cat_shifts.items()},
        "drift_status": "STABLE" if low_confidence_pct < 15.0 else "DRIFT_ALERT"
    }
