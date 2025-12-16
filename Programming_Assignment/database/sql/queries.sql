-- database/sql/queries.sql
-- =========================
-- Q1) Traffic flow: top 10 busiest sites by total flow
SELECT site,
    SUM(flow) AS total_flow
FROM analytics.traffic_flow
WHERE flow IS NOT NULL
GROUP BY site
ORDER BY total_flow DESC
LIMIT 10;
-- The total traffic flow was aggregated by monitoring site to identify the busiest locations.
-- Key findings:
-- 	•	Traffic flow is highly concentrated in a small number of sites.
-- 	•	The busiest site (N03111C) recorded approximately 17.4 million vehicle counts.
-- 	•	The top 10 sites all exceed 10 million total flow, indicating major arterial routes with consistently high demand.
-- Interpretation:
-- This suggests that traffic volume in the study area is dominated by a limited set of critical road segments, which are likely priority locations for traffic management and congestion mitigation.
-- Q2) Traffic flow: hourly pattern (approx) using start_time hour
SELECT EXTRACT(
        HOUR
        FROM start_time
    ) AS hour,
    AVG(flow) AS avg_flow,
    AVG(cong) AS avg_congestion,
    AVG(dsat) AS avg_saturation
FROM analytics.traffic_flow
WHERE start_time IS NOT NULL
GROUP BY hour
ORDER BY hour;
-- Key findings:
-- 	•	Traffic activity is minimal between 00:00–05:00, with very low average flow and congestion.
-- 	•	A sharp increase begins around 06:00, corresponding to the morning commute.
-- 	•	Peak traffic occurs between 12:00 and 15:00, where:
-- 	•	Average flow exceeds 230 vehicles
-- 	•	Congestion and saturation reach their highest levels
-- 	•	Traffic gradually declines after 17:00, with a significant drop in the late evening.
-- Interpretation:
-- The results clearly reflect daily commuting behaviour, with strong daytime peaks and reduced nighttime activity. Congestion and saturation closely follow traffic volume, confirming their dependence on demand levels.
-- Q3) Drivers: injury severity distribution(Uppercased filed values) 
SELECT COALESCE(
        NULLIF(UPPER(TRIM(injury_severity)), ''),
        'UNKNOWN'
    ) AS injury_severity_norm,
    COUNT(*) AS cnt
FROM analytics.crash_drivers
GROUP BY injury_severity_norm
ORDER BY cnt DESC;
-- Key findings:
-- 	•	The majority of driver records (~336,000) report NO APPARENT INJURY.
-- 	•	POSSIBLE INJURY and SUSPECTED MINOR INJURY together account for a substantial minority of cases.
-- 	•	SUSPECTED SERIOUS INJURY and FATAL INJURY are rare, with fatal injuries representing a very small proportion of records.
-- 	•	Approximately 4,700 records are categorised as UNKNOWN, indicating missing or ambiguous reporting.
-- Interpretation:
-- Most traffic crashes result in minor or no injuries, while severe outcomes are comparatively uncommon. The presence of inconsistent injury labels highlights a common real-world data quality issue.
-- Q4) Drivers: at-fault vs not at-fault (and missing)
SELECT driver_at_fault,
    COUNT(*) AS cnt
FROM analytics.crash_drivers
GROUP BY driver_at_fault
ORDER BY cnt DESC;
-- Key findings:
-- 	•	At-fault drivers: ~208,000 records
-- 	•	Not at-fault drivers: ~196,000 records
-- 	•	Unknown: ~9,300 records
-- Interpretation:
-- The distribution between at-fault and not-at-fault drivers is relatively balanced, suggesting that responsibility is often shared or context-dependent rather than dominated by one group.
-- Q5) Drivers: top 10 municipalities with most driver records
SELECT municipality,
    COUNT(*) AS cnt
FROM analytics.crash_drivers
WHERE municipality IS NOT NULL
    AND TRIM(municipality) <> ''
GROUP BY municipality
ORDER BY cnt DESC
LIMIT 10;
-- Key findings:
-- 	•	Rockville and Gaithersburg dominate the dataset, with significantly higher counts than other municipalities.
-- 	•	Smaller municipalities contribute comparatively few records.
-- Interpretation:
-- Crash driver records are heavily concentrated in a small number of urban areas, likely reflecting higher population density and traffic exposure.
-- Q6) Cary incidents: accidents by weather condition
SELECT weather,
    COUNT(*) AS cnt
FROM analytics.crash_incidents
GROUP BY weather
ORDER BY cnt DESC;
-- Key findings:
-- 	•	The majority of crashes occur under CLEAR weather conditions.
-- 	•	Adverse weather conditions (rain, snow, fog, severe winds) account for a much smaller share of incidents.
-- 	•	A small number of records contain missing or blank weather values.
-- Interpretation:
-- Crash frequency appears to be driven more by traffic exposure than by adverse weather, as most incidents occur during normal driving conditions.
-- Q7) Cary incidents: accidents by month/year (based on crash_date)
SELECT DATE_TRUNC('month', crash_date) AS month,
    COUNT(*) AS cnt
FROM analytics.crash_incidents
WHERE crash_date IS NOT NULL
GROUP BY DATE_TRUNC('month', crash_date)
ORDER BY month;
-- # Monthly crash analysis (2020–2024):
-- # - Crash counts range mostly between 600–800 per month.
-- # - Noticeably lower values in early 2020, likely due to reduced traffic.
-- # - Clear seasonal pattern with higher counts in autumn months.
-- # - Overall stable but slightly increasing trend over time.
-- Q8) Cary incidents: share of fatality / injury (simple indicators)
SELECT SUM(
        CASE
            WHEN fatality IS NOT NULL
            AND fatality > 0 THEN 1
            ELSE 0
        END
    ) AS fatal_cases,
    SUM(
        CASE
            WHEN possible_injury IS NOT NULL
            AND possible_injury > 0 THEN 1
            ELSE 0
        END
    ) AS possible_injury_cases,
    COUNT(*) AS total_cases
FROM analytics.crash_incidents;
-- Key findings:
-- 	•	Fatal cases: 68
-- 	•	Possible injury cases: 6,180
-- 	•	Total incidents: 47,144
-- Interpretation:
-- Fatal crashes represent a very small fraction of total incidents, while injury-related cases account for a minority. This reinforces the observation that most crashes do not result in severe outcomes.