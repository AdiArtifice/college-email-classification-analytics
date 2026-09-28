-- ==============================================================================
-- Intelligent College Email Classification — Hive DDL Schema
-- Database: email_analytics
-- External Table: email_features over HDFS features/emails.parquet
-- ==============================================================================

CREATE DATABASE IF NOT EXISTS email_analytics;
USE email_analytics;

DROP TABLE IF EXISTS email_features;

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
LOCATION '/user/student/email_project/features/emails.parquet';
