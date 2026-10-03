SELECT COUNT(*) AS total_trips
FROM fact_trips;

SELECT 
    month_name,
    COUNT(*) AS total_trips
FROM fact_trips
GROUP BY month, month_name
ORDER BY month;

SELECT 
    hour,
    COUNT(*) AS total_trips
FROM fact_trips
GROUP BY hour
ORDER BY total_trips DESC;

SELECT 
    b.base_code,
    COUNT(*) AS total_trips
FROM fact_trips f
JOIN dim_base b
    ON f.base_id = b.base_id
GROUP BY b.base_code
ORDER BY total_trips DESC;

SELECT
    CASE
        WHEN is_weekend = 1 THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    COUNT(*) AS total_trips
FROM fact_trips
GROUP BY is_weekend
ORDER BY is_weekend;

SELECT
    weekday_name,
    COUNT(*) AS total_trips
FROM fact_trips
GROUP BY weekday_num, weekday_name
ORDER BY total_trips DESC;

SELECT
    month_name,
    hour,
    COUNT(*) AS total_trips
FROM fact_trips
GROUP BY month, month_name, hour
ORDER BY total_trips DESC
LIMIT 15;

-- Share of trips per base (window function)
SELECT b.base_code,
       COUNT(*) AS trips,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM fact_trips f
JOIN dim_base b ON b.base_id = f.base_id
GROUP BY b.base_code
ORDER BY trips DESC;

-- Daily trend using dim_date
SELECT d.trip_date, COUNT(*) AS trips
FROM fact_trips f
JOIN dim_date d ON d.date_id = f.date_id
GROUP BY d.trip_date
ORDER BY d.trip_date;

-- Weekday x hour heatmap data
SELECT weekday_num, weekday_name, hour, COUNT(*) AS trips
FROM fact_trips
GROUP BY weekday_num, weekday_name, hour
ORDER BY weekday_num, hour;

-- Pickup hotspots on a ~1km grid (rounding lat/lon to 2 decimals)
SELECT ROUND(lat, 2) AS lat_bin, ROUND(lon, 2) AS lon_bin, COUNT(*) AS trips
FROM fact_trips
GROUP BY lat_bin, lon_bin
ORDER BY trips DESC
LIMIT 20;

-- Month-over-month growth
WITH m AS (
  SELECT month, month_name, COUNT(*) AS trips
  FROM fact_trips GROUP BY month, month_name
)
SELECT month_name, trips,	
       ROUND(100.0 * (trips - LAG(trips) OVER (ORDER BY month))
             / LAG(trips) OVER (ORDER BY month), 1) AS growth_pct
FROM m
ORDER BY month;