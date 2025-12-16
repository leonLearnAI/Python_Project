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
-- Q3) Drivers: injury severity distribution(Uppercased filed values) 
SELECT COALESCE(
        NULLIF(UPPER(TRIM(injury_severity)), ''),
        'UNKNOWN'
    ) AS injury_severity_norm,
    COUNT(*) AS cnt
FROM analytics.crash_drivers
GROUP BY injury_severity_norm
ORDER BY cnt DESC;
-- Q4) Drivers: at-fault vs not at-fault (and missing)
SELECT driver_at_fault,
    COUNT(*) AS cnt
FROM analytics.crash_drivers
GROUP BY driver_at_fault
ORDER BY cnt DESC;
-- Q5) Drivers: top 10 municipalities with most driver records
SELECT municipality,
    COUNT(*) AS cnt
FROM analytics.crash_drivers
WHERE municipality IS NOT NULL
    AND TRIM(municipality) <> ''
GROUP BY municipality
ORDER BY cnt DESC
LIMIT 10;
-- Q6) Cary incidents: accidents by weather condition
SELECT weather,
    COUNT(*) AS cnt
FROM analytics.crash_incidents
GROUP BY weather
ORDER BY cnt DESC;
-- Q7) Cary incidents: accidents by month/year (based on crash_date)
SELECT DATE_TRUNC('month', crash_date) AS month,
    COUNT(*) AS cnt
FROM analytics.crash_incidents
WHERE crash_date IS NOT NULL
GROUP BY month
ORDER BY month;
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