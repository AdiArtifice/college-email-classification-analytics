"""
Hive Analytics Execution and Result Materialization (Phase 6).

Executes the 7 analytical queries against the Hive external table `email_analytics.email_features`
and materializes the results into both CSV and Parquet formats in `data/output/analytics/`
for instant consumption by the Streamlit dashboard (Phase 7).
"""

import os
import sys
import json
import time
import pandas as pd

# Ensure project root is in sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, '..'))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from pyspark.sql import SparkSession

ANALYTICS_DIR = os.path.join(PROJECT_ROOT, 'data', 'output', 'analytics')
os.makedirs(ANALYTICS_DIR, exist_ok=True)


def get_spark_hive_session():
    """Create SparkSession with Hive Metastore support."""
    return SparkSession.builder \
        .appName("EmailHiveAnalytics") \
        .config("spark.sql.warehouse.dir", "/user/hive/warehouse") \
        .config("spark.ui.enabled", "false") \
        .enableHiveSupport() \
        .getOrCreate()


def setup_external_table(spark: SparkSession):
    """Ensure the external Hive table is created over HDFS features parquet."""
    print("[Hive Setup] Initializing external table...")
    spark.sql("CREATE DATABASE IF NOT EXISTS email_analytics")
    spark.sql("USE email_analytics")
    spark.sql("DROP TABLE IF EXISTS email_features")
    
    ddl = """
    CREATE EXTERNAL TABLE IF NOT EXISTS email_features (
        message_id STRING,
        thread_id STRING,
        sender STRING,
        sender_email STRING,
        sender_domain STRING,
        recipients ARRAY<STRING>,
        subject STRING,
        date_utc STRING,
        body_text STRING,
        body_length INT,
        has_attachment BOOLEAN,
        attachment_count INT,
        gmail_labels ARRAY<STRING>,
        is_unread BOOLEAN,
        is_spam BOOLEAN,
        is_sent BOOLEAN,
        is_important BOOLEAN,
        cleaned_text STRING,
        ml_category STRING,
        ml_confidence FLOAT,
        category STRING,
        confidence DOUBLE,
        prediction_method STRING,
        rule_category STRING,
        campus_type STRING,
        organization STRING,
        opportunity_type STRING,
        deadline STRING,
        event_date STRING,
        keywords ARRAY<STRING>
    )
    STORED AS PARQUET
    LOCATION '/user/student/email_project/features/emails.parquet'
    """
    spark.sql(ddl)
    print("[Hive Setup] External table `email_analytics.email_features` ready.")


def run_and_save_query(spark: SparkSession, query: str, name: str, description: str) -> pd.DataFrame:
    """Execute SQL query, log results, and materialize to CSV and Parquet."""
    print("\n" + "=" * 75)
    print(f"Executing: {description}")
    print("=" * 75)
    
    df_spark = spark.sql(query)
    df_pd = df_spark.toPandas()
    
    # Display preview in console
    print(df_pd.head(15).to_string(index=False))
    print(f"Total rows: {len(df_pd)}")

    # Materialize to CSV
    csv_path = os.path.join(ANALYTICS_DIR, f"{name}.csv")
    df_pd.to_csv(csv_path, index=False)
    
    # Materialize to Parquet
    parquet_path = os.path.join(ANALYTICS_DIR, f"{name}.parquet")
    df_pd.to_parquet(parquet_path, index=False)
    
    print(f"Saved: {csv_path} & {parquet_path}")
    return df_pd


def main():
    start_time = time.time()
    spark = get_spark_hive_session()
    spark.sparkContext.setLogLevel("WARN")

    setup_external_table(spark)

    # -------------------------------------------------------------
    # Query 1: Count by Category
    # -------------------------------------------------------------
    q1 = """
    SELECT 
        category,
        COUNT(*) AS email_count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
    FROM email_features
    GROUP BY category
    ORDER BY email_count DESC
    """
    df_q1 = run_and_save_query(spark, q1, "category_counts", "Query 1: Email Count by Category")

    # -------------------------------------------------------------
    # Query 2: Month-over-Month Volume Trend
    # -------------------------------------------------------------
    q2 = """
    SELECT 
        SUBSTR(date_utc, 1, 7) AS year_month,
        COUNT(*) AS total_emails,
        SUM(CASE WHEN category IN ('Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event') THEN 1 ELSE 0 END) AS opportunity_emails,
        SUM(CASE WHEN is_unread = TRUE THEN 1 ELSE 0 END) AS unread_emails
    FROM email_features
    GROUP BY SUBSTR(date_utc, 1, 7)
    ORDER BY year_month ASC
    """
    df_q2 = run_and_save_query(spark, q2, "monthly_volume", "Query 2: Month-over-Month Volume Trend")

    # -------------------------------------------------------------
    # Query 3: Opportunity Category and Domain Breakdown
    # -------------------------------------------------------------
    q3 = """
    SELECT 
        category,
        COALESCE(opportunity_type, 'General Tech') AS opportunity_type,
        COUNT(*) AS count
    FROM email_features
    WHERE category IN ('Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event')
    GROUP BY category, COALESCE(opportunity_type, 'General Tech')
    ORDER BY category, count DESC
    """
    df_q3 = run_and_save_query(spark, q3, "opportunity_breakdown", "Query 3: Opportunity Category & Sub-domain Breakdown")

    # -------------------------------------------------------------
    # Query 4: On-Campus vs Off-Campus
    # -------------------------------------------------------------
    q4 = """
    SELECT 
        category,
        COALESCE(campus_type, 'Off-campus') AS campus_type,
        COUNT(*) AS count,
        ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY category), 1) AS pct_within_category
    FROM email_features
    WHERE category IN ('Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event', 'Academic', 'Examination')
    GROUP BY category, COALESCE(campus_type, 'Off-campus')
    ORDER BY category, count DESC
    """
    df_q4 = run_and_save_query(spark, q4, "campus_distribution", "Query 4: On-Campus vs Off-Campus Opportunities")

    # -------------------------------------------------------------
    # Query 5: Read vs Unread Distribution
    # -------------------------------------------------------------
    q5 = """
    SELECT 
        category,
        COUNT(*) AS total_emails,
        SUM(CASE WHEN is_unread = TRUE THEN 1 ELSE 0 END) AS unread_count,
        SUM(CASE WHEN is_unread = FALSE THEN 1 ELSE 0 END) AS read_count,
        ROUND(SUM(CASE WHEN is_unread = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS unread_percentage
    FROM email_features
    GROUP BY category
    ORDER BY total_emails DESC
    """
    df_q5 = run_and_save_query(spark, q5, "read_unread_distribution", "Query 5: Read vs Unread Distribution Across Categories")

    # -------------------------------------------------------------
    # Query 6: Top Senders and Organizations
    # -------------------------------------------------------------
    q6 = """
    SELECT 
        sender_domain,
        COALESCE(organization, 'Other') AS organization,
        COUNT(*) AS total_emails,
        SUM(CASE WHEN category IN ('Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event') THEN 1 ELSE 0 END) AS opportunity_count
    FROM email_features
    GROUP BY sender_domain, COALESCE(organization, 'Other')
    ORDER BY total_emails DESC
    LIMIT 25
    """
    df_q6 = run_and_save_query(spark, q6, "top_senders", "Query 6: Top Sender Domains & Identified Organizations")

    # -------------------------------------------------------------
    # Query 7: Upcoming Deadlines
    # -------------------------------------------------------------
    q7 = """
    SELECT 
        deadline,
        category,
        organization,
        campus_type,
        opportunity_type,
        subject
    FROM email_features
    WHERE deadline IS NOT NULL
    ORDER BY deadline DESC
    """
    df_q7 = run_and_save_query(spark, q7, "upcoming_deadlines", "Query 7: Active and Historical Opportunity Deadlines")

    # Overall Summary KPIs for Streamlit Dashboard
    summary_kpis = {
        "total_emails": int(df_q1['email_count'].sum()),
        "total_categories": len(df_q1),
        "total_opportunities": int(df_q3['count'].sum()),
        "total_deadlines": len(df_q7),
        "unread_emails": int(df_q5['unread_count'].sum()),
        "read_emails": int(df_q5['read_count'].sum()),
        "unread_percentage": round(df_q5['unread_count'].sum() * 100.0 / df_q1['email_count'].sum(), 2),
        "top_category": str(df_q1.iloc[0]['category']),
        "top_category_count": int(df_q1.iloc[0]['email_count']),
        "elapsed_seconds": round(time.time() - start_time, 2)
    }

    kpi_path = os.path.join(ANALYTICS_DIR, "analytics_summary.json")
    with open(kpi_path, 'w', encoding='utf-8') as f:
        json.dump(summary_kpis, f, indent=2)

    print("\n" + "=" * 75)
    print(" Hive Analytics Suite Completed Successfully! 🎉")
    print(f" Total execution time: {summary_kpis['elapsed_seconds']}s")
    print(f" Summary KPIs saved to: {kpi_path}")
    print("=" * 75)

    spark.stop()


if __name__ == '__main__':
    main()
