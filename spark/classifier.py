"""
Spark MLlib TF-IDF feature extraction and email classification.

Supports:
- HashingTF + IDF feature extraction
- Multinomial Logistic Regression and Naive Bayes classifiers
- Model evaluation (Accuracy, Weighted F1, Weighted Precision, Weighted Recall)
- Hybrid classification resolution (rules for high confidence, ML for the rest)
- Confidence score extraction from ML probability vectors
"""

from typing import Dict, Any, Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, udf
from pyspark.sql.types import FloatType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import (
    RegexTokenizer,
    StopWordsRemover,
    HashingTF,
    IDF,
    StringIndexer,
    StringIndexerModel,
    IndexToString,
)
from pyspark.ml.classification import LogisticRegression, NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator


def build_feature_pipeline(num_features: int = 20000) -> Pipeline:
    """
    Build a feature extraction pipeline:
    RegexTokenizer -> StopWordsRemover -> HashingTF -> IDF
    """
    tokenizer = RegexTokenizer(
        inputCol='cleaned_text',
        outputCol='tokens',
        minTokenLength=3,
        pattern=r'\s+'
    )
    stopwords_remover = StopWordsRemover(
        inputCol='tokens',
        outputCol='filtered_tokens'
    )
    hashing_tf = HashingTF(
        inputCol='filtered_tokens',
        outputCol='raw_features',
        numFeatures=num_features
    )
    idf = IDF(
        inputCol='raw_features',
        outputCol='features'
    )

    return Pipeline(stages=[tokenizer, stopwords_remover, hashing_tf, idf])


def train_classifier(
    train_df: DataFrame,
    model_type: str = 'lr',
    reg_param: float = 0.01,
    max_iter: int = 25
) -> Tuple[Any, StringIndexerModel]:
    """
    Train a classifier (Logistic Regression or Naive Bayes) on labeled data.

    Args:
        train_df: DataFrame containing 'features' and 'rule_label'
        model_type: 'lr' for Logistic Regression, 'nb' for Naive Bayes
        reg_param: Regularization parameter for Logistic Regression
        max_iter: Max iterations for Logistic Regression

    Returns:
        Tuple of (trained_model, string_indexer_model)
    """
    indexer = StringIndexer(inputCol='rule_label', outputCol='label')
    indexer_model = indexer.fit(train_df)
    indexed_train = indexer_model.transform(train_df)

    model_type_lower = model_type.lower()
    if model_type_lower == 'nb':
        estimator = NaiveBayes(
            featuresCol='features',
            labelCol='label',
            modelType='multinomial'
        )
    else:
        estimator = LogisticRegression(
            featuresCol='features',
            labelCol='label',
            maxIter=max_iter,
            regParam=reg_param,
            elasticNetParam=0.0
        )

    model = estimator.fit(indexed_train)
    return model, indexer_model


def evaluate_model(
    model: Any,
    test_df: DataFrame,
    indexer_model: StringIndexerModel
) -> Dict[str, Any]:
    """
    Evaluate trained classifier on a held-out test split.
    """
    indexed_test = indexer_model.transform(test_df)
    predictions = model.transform(indexed_test)

    eval_acc = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='accuracy')
    eval_f1 = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='f1')
    eval_prec = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='weightedPrecision')
    eval_rec = MulticlassClassificationEvaluator(labelCol='label', predictionCol='prediction', metricName='weightedRecall')

    accuracy = float(eval_acc.evaluate(predictions))
    f1 = float(eval_f1.evaluate(predictions))
    precision = float(eval_prec.evaluate(predictions))
    recall = float(eval_rec.evaluate(predictions))

    return {
        'accuracy': round(accuracy, 4),
        'f1_score': round(f1, 4),
        'weighted_precision': round(precision, 4),
        'weighted_recall': round(recall, 4),
        'labels': indexer_model.labels
    }


@udf(returnType=FloatType())
def _extract_max_prob(prob_vector):
    """Extract max class probability from Spark MLlib probability vector."""
    if prob_vector is not None:
        try:
            return float(max(prob_vector))
        except Exception:
            return 0.0
    return 0.0


def apply_hybrid_classification(
    df_with_features: DataFrame,
    model: Any,
    indexer_model: StringIndexerModel
) -> DataFrame:
    """
    Apply ML model across all records and combine with rule labels into a hybrid prediction:
    - If rule_label is present: category = rule_label, confidence = 1.0, method = 'rule'
    - If rule_label is null: category = ml_category, confidence = ml_probability, method = 'ml'
    """
    # 1. Inference on all records
    predictions = model.transform(df_with_features)

    # 2. Map predicted label index back to string category
    idx_to_str = IndexToString(
        inputCol='prediction',
        outputCol='ml_category',
        labels=indexer_model.labels
    )
    mapped_df = idx_to_str.transform(predictions)

    # 3. Extract max probability for ML confidence
    enriched_df = mapped_df.withColumn('ml_confidence', _extract_max_prob(col('probability')))

    # 4. Resolve hybrid category, confidence, and prediction method
    hybrid_df = enriched_df.withColumn(
        'category',
        when(col('rule_label').isNotNull(), col('rule_label')).otherwise(col('ml_category'))
    ).withColumn(
        'confidence',
        when(col('rule_label').isNotNull(), lit(1.0)).otherwise(col('ml_confidence'))
    ).withColumn(
        'prediction_method',
        when(col('rule_label').isNotNull(), lit('rule')).otherwise(lit('ml'))
    ).withColumn(
        'rule_category',
        col('rule_label')
    )

    # 5. Clean up internal MLlib intermediate vector columns
    # Dropping these ensures the output Parquet schema is 100% clean and compatible with Hive
    cols_to_drop = [
        'tokens', 'filtered_tokens', 'raw_features', 'features',
        'rawPrediction', 'probability', 'prediction', 'rule_label'
    ]
    for col_name in cols_to_drop:
        if col_name in hybrid_df.columns:
            hybrid_df = hybrid_df.drop(col_name)

    return hybrid_df
