-- ==============================================================================
-- Intelligent College Email Classification — Hive Analytical Queries (Phase 6)
-- Database: email_analytics
-- Target Table: email_features
-- ==============================================================================

USE email_analytics;

-- ------------------------------------------------------------------------------
-- Query 1: Email Count and Percentage by Category
-- Answers: What is the high-level distribution of messages in the inbox?
-- ------------------------------------------------------------------------------
SELECT 
    category,
    COUNT(*) AS email_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM email_features
GROUP BY category
ORDER BY email_count DESC;

-- ------------------------------------------------------------------------------
-- Query 2: Month-over-Month Volume Trend
-- Answers: How is email volume growing over time, and when do opportunities arrive?
-- Demonstrates: Big Data 5 Vs (Volume and Velocity trajectory)
-- ------------------------------------------------------------------------------
SELECT 
    SUBSTR(date_utc, 1, 7) AS year_month,
    COUNT(*) AS total_emails,
    SUM(CASE WHEN category IN ('Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event') THEN 1 ELSE 0 END) AS opportunity_emails,
    SUM(CASE WHEN is_unread = TRUE THEN 1 ELSE 0 END) AS unread_emails
FROM email_features
GROUP BY SUBSTR(date_utc, 1, 7)
ORDER BY year_month ASC;

-- ------------------------------------------------------------------------------
-- Query 3: Opportunity Category and Technical Domain Breakdown
-- Answers: What types of technical opportunities are available (AI/ML, Web, Data)?
-- ------------------------------------------------------------------------------
SELECT 
    category,
    COALESCE(opportunity_type, 'General Tech') AS opportunity_type,
    COUNT(*) AS count
FROM email_features
WHERE category IN ('Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event')
GROUP BY category, COALESCE(opportunity_type, 'General Tech')
ORDER BY category, count DESC;

-- ------------------------------------------------------------------------------
-- Query 4: On-Campus vs Off-Campus Opportunity Distribution
-- Answers: Are student opportunities sourced directly by the college or externally?
-- ------------------------------------------------------------------------------
SELECT 
    category,
    COALESCE(campus_type, 'Off-campus') AS campus_type,
    COUNT(*) AS count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(PARTITION BY category), 1) AS pct_within_category
FROM email_features
WHERE category IN ('Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event', 'Academic', 'Examination')
GROUP BY category, COALESCE(campus_type, 'Off-campus')
ORDER BY category, count DESC;

-- ------------------------------------------------------------------------------
-- Query 5: Read vs Unread Distribution Across Categories
-- Answers: What kinds of emails does the student actually open versus ignore?
-- ------------------------------------------------------------------------------
SELECT 
    category,
    COUNT(*) AS total_emails,
    SUM(CASE WHEN is_unread = TRUE THEN 1 ELSE 0 END) AS unread_count,
    SUM(CASE WHEN is_unread = FALSE THEN 1 ELSE 0 END) AS read_count,
    ROUND(SUM(CASE WHEN is_unread = TRUE THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS unread_percentage
FROM email_features
GROUP BY category
ORDER BY total_emails DESC;

-- ------------------------------------------------------------------------------
-- Query 6: Top Sender Domains & Organizations
-- Answers: Who sends the most emails and which organizations offer the most opportunities?
-- ------------------------------------------------------------------------------
SELECT 
    sender_domain,
    COALESCE(organization, 'Other') AS organization,
    COUNT(*) AS total_emails,
    SUM(CASE WHEN category IN ('Internship', 'Job/Placement', 'Interview', 'Hackathon/Competition', 'Workshop/Event') THEN 1 ELSE 0 END) AS opportunity_count
FROM email_features
GROUP BY sender_domain, COALESCE(organization, 'Other')
ORDER BY total_emails DESC
LIMIT 20;

-- ------------------------------------------------------------------------------
-- Query 7: Opportunity Deadlines
-- Answers: What upcoming deadlines should the student be aware of?
-- ------------------------------------------------------------------------------
SELECT 
    deadline,
    category,
    organization,
    campus_type,
    opportunity_type,
    subject
FROM email_features
WHERE deadline IS NOT NULL
ORDER BY deadline DESC;
