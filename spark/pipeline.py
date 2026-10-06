"""
Spark Pipeline Driver for Intelligent College Email Classification and Opportunity Analytics System.

End-to-End Pipeline:
1. Load `cleaned/` Parquet from HDFS or local filesystem.
2. Text preprocessing (lowercase, strip quoted replies, remove CSS/URLs/signatures, spell-light norm).
3. TF-IDF feature extraction (RegexTokenizer + StopWordsRemover + HashingTF + IDF).
4. Weak supervision labeling (Gmail Category bootstrap + domain/keyword rules + forum handling).
5. Train and evaluate Spark MLlib classifier (Logistic Regression or Naive Bayes).
6. Hybrid resolution (high-confidence rules for labeled subset, ML inference for remainder) with confidence.
7. Information extraction (campus_type, organization, opportunity_type, deadline, event_date, keywords).
8. Persist semantically enriched `features/` Parquet to HDFS and local filesystem.
"""

import os
import sys
import time
import json
import argparse
from typing import Dict, Any

# Ensure project root is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

from spark.preprocess import get_clean_text_udf
from spark.labeling import get_weak_label_udf
from spark.classifier import (
    build_feature_pipeline,
    train_classifier,
    evaluate_model,
    apply_hybrid_classification,
)
from spark.extract import enrich_opportunities


def parse_args():
    parser = argparse.ArgumentParser(description="Run Spark Email Classification & Opportunity Pipeline")
    parser.add_argument(
        '--input',
        default='hdfs://localhost:9000/user/student/email_project/cleaned/emails.parquet',
        help='Input cleaned parquet path (HDFS or local URI)'
    )
    parser.add_argument(
        '--output',
        default='hdfs://localhost:9000/user/student/email_project/features/emails.parquet',
        help='Output features parquet path (HDFS or local URI)'
    )
    parser.add_argument(
        '--local-output',
        default='data/output/features/emails.parquet',
        help='Optional local parquet output path for offline inspection (set empty to skip)'
    )
    parser.add_argument(
        '--summary-output',
        default='data/output/features_summary.json',
        help='Path to save pipeline execution summary JSON'
    )
    parser.add_argument(
        '--model-type',
        choices=['lr', 'nb'],
        default='lr',
        help='MLlib classifier type: "lr" (Logistic Regression) or "nb" (Naive Bayes)'
    )
    parser.add_argument(
        '--eval',
        action='store_true',
        default=True,
        help='Evaluate model on an 80/20 train/test split of weak-supervision data'
    )
    parser.add_argument(
        '--no-extract',
        action='store_true',
        default=False,
        help='Skip Phase 5 information extraction (campus_type, organization, deadline, etc.)'
    )
    parser.add_argument(
        '--track-mlflow',
        action='store_true',
        default=True,
        help='Track run parameters, metrics, and models in MLflow'
    )
    parser.add_argument(
        '--experiment-name',
        default='College_Email_Classification',
        help='MLflow experiment name'
    )
    parser.add_argument(
        '--register-model',
        action='store_true',
        default=True,
        help='Register candidate model in MLflow Model Registry'
    )
    parser.add_argument(
        '--min-f1',
        type=float,
        default=0.85,
        help='Minimum test F1 score required for production promotion gate'
    )
    return parser.parse_args()


def run_pipeline():
    start_time = time.time()
    args = parse_args()

    print("=" * 70)
    print("Intelligent College Email Classification — Spark Pipeline (Phase 4)")
    print("=" * 70)
    print(f"Input path:          {args.input}")
    print(f"HDFS Output path:    {args.output}")
    print(f"Local Output path:   {args.local_output or 'None'}")
    print(f"Model type:          {args.model_type.upper()}")
    print(f"Run evaluation:      {args.eval}")
    print(f"Information extract: {not args.no_extract}")
    print("=" * 70)

    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("EmailClassificationPipeline") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "4") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 1. Load cleaned Parquet
    print("\n[Step 1/7] Loading cleaned Parquet...")
    df_raw = spark.read.parquet(args.input)
    total_count = df_raw.count()
    print(f"  Loaded {total_count} records from {args.input}")

    if total_count < 7100:
        print(f"  WARNING: Expected >= 7,100 records, found {total_count}")

    # 2. Text Preprocessing
    print("\n[Step 2/7] Preprocessing text (normalizing, stripping quoted text, CSS, URLs, signatures)...")
    clean_udf = get_clean_text_udf()
    df_clean = df_raw.withColumn('cleaned_text', clean_udf(col('subject'), col('body_text')))

    # 3. Weak Supervision Labeling
    print("\n[Step 3/7] Applying weak supervision rules (domain + keyword + Gmail bootstrap)...")
    label_udf = get_weak_label_udf()
    df_labeled = df_clean.withColumn(
        'rule_label',
        label_udf(col('sender_domain'), col('subject'), col('body_text'), col('gmail_labels'))
    )

    # Cache preprocessed DataFrame for multiple passes (feature extraction & training)
    df_labeled.cache()

    labeled_count = df_labeled.filter(col('rule_label').isNotNull()).count()
    unlabeled_count = df_labeled.filter(col('rule_label').isNull()).count()
    print(f"  Rule-labeled records:  {labeled_count} ({labeled_count / total_count * 100:.1f}%)")
    print(f"  Unlabeled records:     {unlabeled_count} ({unlabeled_count / total_count * 100:.1f}%) -> Handed to ML")

    # 4. Feature Extraction: TF-IDF Pipeline
    print("\n[Step 4/7] Extracting TF-IDF features (RegexTokenizer + StopWordsRemover + HashingTF + IDF)...")
    feat_pipeline = build_feature_pipeline(num_features=20000)
    feat_model = feat_pipeline.fit(df_labeled)
    df_with_features = feat_model.transform(df_labeled)
    df_with_features.cache()

    # 5. Train & Evaluate MLlib Classifier
    eval_metrics: Dict[str, Any] = {}
    labeled_data = df_with_features.filter(col('rule_label').isNotNull())

    if args.eval:
        print(f"\n[Step 5/7] Evaluating {args.model_type.upper()} classifier on 80/20 train/test split...")
        train_split, test_split = labeled_data.randomSplit([0.8, 0.2], seed=42)
        eval_model, eval_indexer = train_classifier(train_split, model_type=args.model_type)
        eval_metrics = evaluate_model(eval_model, test_split, eval_indexer)
        print(f"  Test Accuracy:          {eval_metrics['accuracy'] * 100:.2f}%")
        print(f"  Test Weighted F1:       {eval_metrics['f1_score'] * 100:.2f}%")
        print(f"  Test Weighted Precision:{eval_metrics['weighted_precision'] * 100:.2f}%")
        print(f"  Test Weighted Recall:   {eval_metrics['weighted_recall'] * 100:.2f}%")

    print(f"\n[Step 5/7 (cont.)] Fitting final {args.model_type.upper()} model on full labeled dataset ({labeled_count} records)...")
    final_model, final_indexer = train_classifier(labeled_data, model_type=args.model_type)

    # 6. Apply Hybrid Classification
    print("\n[Step 6/7] Applying hybrid classification across all records...")
    df_classified = apply_hybrid_classification(df_with_features, final_model, final_indexer)

    # Optional: Information Extraction (Phase 5)
    if not args.no_extract:
        print("\n[Step 6/7 (cont.)] Enriching with opportunity metadata (campus_type, organization, deadline, keywords)...")
        df_final = enrich_opportunities(df_classified)
    else:
        df_final = df_classified

    # 7. Persist Features Parquet
    print(f"\n[Step 7/7] Persisting features Parquet...")
    print(f"  Writing to HDFS: {args.output}")
    df_final.write.mode("overwrite").parquet(args.output)
    print("  HDFS write complete.")

    # Write local copy if specified
    if args.local_output:
        local_path = args.local_output
        if not local_path.startswith("file://") and not os.path.isabs(local_path):
            local_path = os.path.join(PROJECT_ROOT, local_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        local_uri = f"file://{local_path}"
        print(f"  Writing local copy: {local_uri}")
        df_final.write.mode("overwrite").parquet(local_uri)
        print("  Local write complete.")

    # Validation & Distribution Summary
    final_count = df_final.count()
    print(f"\nVerification:")
    print(f"  Input Count:  {total_count}")
    print(f"  Output Count: {final_count}")
    assert final_count == total_count, f"Row count mismatch! Expected {total_count}, got {final_count}"
    print("  Row count integrity check: PASSED ✅")

    # Category distribution
    cat_distribution = {
        row['category']: row['count']
        for row in df_final.groupBy('category').count().orderBy(col('count').desc()).collect()
    }
    method_distribution = {
        row['prediction_method']: row['count']
        for row in df_final.groupBy('prediction_method').count().collect()
    }

    print("\nCategory Distribution:")
    for cat, cnt in cat_distribution.items():
        print(f"  {cat:25s}: {cnt:5d} ({cnt / final_count * 100:5.1f}%)")

    print("\nPrediction Method Breakdown:")
    for m, cnt in method_distribution.items():
        print(f"  {m:10s}: {cnt:5d} ({cnt / final_count * 100:5.1f}%)")

    elapsed_time = round(time.time() - start_time, 2)
    print(f"\nPipeline completed successfully in {elapsed_time}s! 🎉")

    # MLOps Experiment Tracking & Model Registry
    mlops_info = None
    if args.track_mlflow:
        print("\n[Step 7/7 (cont.)] Logging run parameters, metrics, and models to MLflow...")
        try:
            from mlops.tracker import log_training_run
            mlops_info = log_training_run(
                experiment_name=args.experiment_name,
                run_name=f"spark_mllib_{args.model_type.upper()}_{int(time.time())}",
                model_type=args.model_type,
                hyperparams={
                    "reg_param": 0.01 if args.model_type == 'lr' else 1.0,
                    "max_iter": 25 if args.model_type == 'lr' else 0,
                    "num_features": 20000,
                    "model_family": "multinomial"
                },
                metrics={
                    "accuracy": eval_metrics.get("accuracy", 0.0),
                    "f1_score": eval_metrics.get("f1_score", 0.0),
                    "weighted_precision": eval_metrics.get("weighted_precision", 0.0),
                    "weighted_recall": eval_metrics.get("weighted_recall", 0.0),
                    "elapsed_time": elapsed_time
                },
                dataset_summary={
                    "total_records": final_count,
                    "labeled_records": labeled_count,
                    "unlabeled_records": unlabeled_count
                },
                category_distribution=cat_distribution,
                spark_model=final_model,
                register_model=args.register_model,
                min_f1_threshold=args.min_f1
            )
            print(f"  [MLOps] MLflow Run ID:       {mlops_info['run_id']}")
            print(f"  [MLOps] Production Gate:     {'PASSED ✅' if mlops_info['passed_gate'] else 'REJECTED ❌'}")
            print(f"  [MLOps] Promotion Stage:     {mlops_info['promotion_status']}")
        except Exception as e:
            print(f"  [MLOps WARNING] MLflow logging encountered: {e}")

    # Save summary JSON
    summary_data = {
        "total_records": final_count,
        "rule_labeled_count": labeled_count,
        "ml_labeled_count": unlabeled_count,
        "model_type": args.model_type,
        "evaluation_metrics": eval_metrics,
        "category_distribution": cat_distribution,
        "prediction_method_distribution": method_distribution,
        "hdfs_output_path": args.output,
        "local_output_path": args.local_output,
        "elapsed_time_seconds": elapsed_time,
        "threshold_passed": final_count >= 7100,
        "mlops": mlops_info
    }

    if args.summary_output:
        summary_path = args.summary_output
        if not os.path.isabs(summary_path):
            summary_path = os.path.join(PROJECT_ROOT, summary_path)
        os.makedirs(os.path.dirname(summary_path), exist_ok=True)
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary_data, f, indent=2)
        print(f"Summary saved to: {summary_path}")

    spark.stop()


if __name__ == '__main__':
    run_pipeline()
