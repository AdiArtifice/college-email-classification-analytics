"""
MLOps Package for Intelligent College Email Classification System.
Provides experiment tracking, model registry management, drift monitoring,
and real-time inference utilities.
"""

from .tracker import init_mlops, log_training_run, get_experiment_runs, get_model_registry_info
from .inference import predict_email
from .drift import analyze_drift

__all__ = [
    'init_mlops',
    'log_training_run',
    'get_experiment_runs',
    'get_model_registry_info',
    'predict_email',
    'analyze_drift'
]
