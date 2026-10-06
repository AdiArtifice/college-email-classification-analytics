"""
MLOps Experiment Tracking & Model Registry Module.
Handles MLflow runs, metric logging, model artifact storage,
and model governance for PySpark MLlib.
"""

import os
import sys
import json
import time
from typing import Dict, Any, Optional, Tuple, List
import pandas as pd

# Set environment variable to silence MLflow hint
os.environ["MLFLOW_DISABLE_AGENT_HINT"] = "1"

# Resolve project root
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
DB_PATH = os.path.join(PROJECT_ROOT, "mlflow.db")
TRACKING_URI = f"sqlite:///{DB_PATH}"

import mlflow
import mlflow.spark
from mlflow.tracking import MlflowClient


def get_tracking_uri() -> str:
    """Return SQLite tracking URI for MLflow."""
    return TRACKING_URI


def init_mlops(experiment_name: str = "College_Email_Classification") -> str:
    """
    Initialize MLflow tracking server connection and return experiment ID.
    """
    mlflow.set_tracking_uri(TRACKING_URI)
    client = MlflowClient()
    experiment = client.get_experiment_by_name(experiment_name)
    if experiment is None:
        experiment_id = client.create_experiment(
            name=experiment_name,
            tags={
                "project": "Intelligent College Email Classification",
                "framework": "PySpark MLlib",
                "cluster": "Hadoop 3.4.1 / Spark 3.5.6"
            }
        )
    else:
        experiment_id = experiment.experiment_id
    mlflow.set_experiment(experiment_name)
    return experiment_id


def log_training_run(
    experiment_name: str,
    run_name: str,
    model_type: str,
    hyperparams: Dict[str, Any],
    metrics: Dict[str, Any],
    dataset_summary: Dict[str, Any],
    category_distribution: Dict[str, int],
    spark_model: Optional[Any] = None,
    register_model: bool = True,
    model_name: str = "CollegeEmailClassifier",
    min_f1_threshold: float = 0.85
) -> Dict[str, Any]:
    """
    Log a complete training, evaluation, and registry run to MLflow.

    Args:
        experiment_name: Name of MLflow experiment
        run_name: Display name for the run
        model_type: 'lr' or 'nb'
        hyperparams: Dictionary of hyperparameters
        metrics: Dictionary of test metrics (accuracy, f1_score, etc.)
        dataset_summary: Counts of total, labeled, and unlabeled records
        category_distribution: Frequency of each category
        spark_model: Optional fitted Spark MLlib PipelineModel or Classifier
        register_model: Whether to register the model in the MLflow Model Registry
        model_name: Registered model name
        min_f1_threshold: Minimum F1 score required to promote model to Production

    Returns:
        Dictionary with run_id, status, and production promotion status
    """
    init_mlops(experiment_name)

    with mlflow.start_run(run_name=run_name) as run:
        run_id = run.info.run_id

        # 1. Log Hyperparameters & Config
        mlflow.log_param("model_type", model_type)
        mlflow.log_param("model_name", "LogisticRegression" if model_type.lower() == 'lr' else "NaiveBayes")
        for k, v in hyperparams.items():
            mlflow.log_param(k, v)

        for k, v in dataset_summary.items():
            mlflow.log_param(f"data_{k}", v)

        # 2. Log Evaluation Metrics
        f1 = metrics.get('f1_score', 0.0)
        acc = metrics.get('accuracy', 0.0)
        prec = metrics.get('weighted_precision', 0.0)
        rec = metrics.get('weighted_recall', 0.0)

        mlflow.log_metric("accuracy", acc)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("weighted_precision", prec)
        mlflow.log_metric("weighted_recall", rec)

        if 'elapsed_time' in metrics:
            mlflow.log_metric("training_duration_seconds", metrics['elapsed_time'])

        # 3. Production Quality Gate Decision
        passed_gate = f1 >= min_f1_threshold
        promotion_status = "Production" if passed_gate else "Candidate"
        mlflow.set_tag("production_gate", "PASSED" if passed_gate else "REJECTED")
        mlflow.set_tag("promotion_status", promotion_status)
        mlflow.set_tag("min_f1_threshold", str(min_f1_threshold))

        # 4. Save Artifacts (JSON summaries)
        artifacts_dir = os.path.join(PROJECT_ROOT, "data", "output", "mlops_artifacts", run_id)
        os.makedirs(artifacts_dir, exist_ok=True)

        metrics_file = os.path.join(artifacts_dir, "metrics.json")
        with open(metrics_file, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)
        mlflow.log_artifact(metrics_file, artifact_path="evaluation")

        cat_file = os.path.join(artifacts_dir, "category_distribution.json")
        with open(cat_file, "w", encoding="utf-8") as f:
            json.dump(category_distribution, f, indent=2)
        mlflow.log_artifact(cat_file, artifact_path="evaluation")

        # 5. Log Spark Model & Register in Model Registry
        model_version = None
        if spark_model is not None:
            try:
                mlflow.spark.log_model(
                    spark_model=spark_model,
                    artifact_path="spark_model"
                )
                print(f"  [MLOps] Spark model artifact logged successfully.")

                if register_model:
                    client = MlflowClient()
                    try:
                        client.get_registered_model(model_name)
                    except Exception:
                        client.create_registered_model(
                            model_name,
                            description="Intelligent College Email Classifier Model Registry"
                        )

                    model_uri = f"runs:/{run_id}/spark_model"
                    mv = client.create_model_version(
                        name=model_name,
                        source=model_uri,
                        run_id=run_id,
                        description=f"{model_type.upper()} model trained on {dataset_summary.get('total_records', 'N/A')} emails. Test F1: {f1*100:.2f}%."
                    )
                    model_version = mv.version
                    client.set_model_version_tag(
                        name=model_name,
                        version=mv.version,
                        key="stage",
                        value=promotion_status
                    )
                    client.set_model_version_tag(
                        name=model_name,
                        version=mv.version,
                        key="f1_score",
                        value=f"{f1:.4f}"
                    )
                    print(f"  [MLOps] Registered in Model Registry as '{model_name}' (Version {mv.version}) -> {promotion_status}")
            except Exception as e:
                print(f"  [MLOps WARNING] Model artifact logging/registration encountered: {e}")

        return {
            "run_id": run_id,
            "experiment_name": experiment_name,
            "model_type": model_type,
            "accuracy": acc,
            "f1_score": f1,
            "passed_gate": passed_gate,
            "promotion_status": promotion_status,
            "model_version": model_version
        }


def get_experiment_runs(experiment_name: str = "College_Email_Classification") -> pd.DataFrame:
    """
    Fetch all runs for an experiment into a pandas DataFrame.
    """
    init_mlops(experiment_name)
    runs_df = mlflow.search_runs(experiment_names=[experiment_name])
    return runs_df


def get_model_registry_info(model_name: str = "CollegeEmailClassifier") -> Dict[str, Any]:
    """
    Retrieve registered model metadata and versions from MLflow.
    """
    init_mlops()
    client = MlflowClient()
    try:
        reg_model = client.get_registered_model(model_name)
        versions = client.search_model_versions(f"name='{model_name}'")
        ver_list = []
        for v in versions:
            ver_list.append({
                "version": v.version,
                "run_id": v.run_id,
                "current_stage": v.tags.get("stage", "None"),
                "status": v.status,
                "description": v.description,
                "creation_timestamp": pd.to_datetime(v.creation_timestamp, unit='ms').strftime('%Y-%m-%d %H:%M:%S'),
                "f1_score": v.tags.get("f1_score", "N/A")
            })
        return {
            "name": reg_model.name,
            "description": reg_model.description,
            "creation_timestamp": pd.to_datetime(reg_model.creation_timestamp, unit='ms').strftime('%Y-%m-%d %H:%M:%S'),
            "versions": ver_list
        }
    except Exception:
        return {
            "name": model_name,
            "description": "Model registry not yet initialized or no versions found.",
            "creation_timestamp": "N/A",
            "versions": []
        }
