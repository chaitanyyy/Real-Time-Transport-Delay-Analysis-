# python/run_queries.py
import pandas as pd
from sqlalchemy import create_engine

MYSQL_PASSWORD = "chaitany41"
engine = create_engine(
    f'mysql+pymysql://root:{MYSQL_PASSWORD}@localhost:3306/transport_analytics'
)
BASE = r'C:\Users\Mane Chaitanya\Desktop\transport-delay-project\data\processed'

# ── Query 2: Route Performance ──
print("Running Query 2: Route Performance...")
q2 = """
SELECT
    route_id,
    COUNT(*)                     AS total_trips,
    ROUND(AVG(delay_minutes),2)  AS avg_delay_mins,
    ROUND(MAX(delay_minutes),2)  AS worst_delay,
    SUM(CASE WHEN delay_minutes > 5
        THEN 1 ELSE 0 END)       AS delayed_trips,
    ROUND(100.0 * SUM(CASE WHEN delay_minutes > 5
        THEN 1 ELSE 0 END)
        / COUNT(*), 1)           AS delay_rate_pct,
    CASE
        WHEN AVG(delay_minutes) > 15 THEN 'Critical'
        WHEN AVG(delay_minutes) > 8  THEN 'High'
        WHEN AVG(delay_minutes) > 3  THEN 'Medium'
        ELSE 'Good'
    END AS performance_label
FROM trips
GROUP BY route_id
HAVING total_trips > 100
ORDER BY avg_delay_mins DESC
LIMIT 50
"""
df2 = pd.read_sql(q2, engine)
df2.to_csv(f'{BASE}\\route_performance.csv', index=False)
print(f"✅ Route performance saved: {len(df2)} routes")
print(df2.head(5))

# ── Query 3: Peak Hour Analysis ──
print("\nRunning Query 3: Peak Hour Analysis...")
q3 = """
SELECT
    is_peak_hour,
    hour_of_day,
    COUNT(*)                     AS total_trips,
    ROUND(AVG(delay_minutes),2)  AS avg_delay,
    ROUND(MIN(delay_minutes),2)  AS min_delay,
    ROUND(MAX(delay_minutes),2)  AS max_delay,
    SUM(CASE WHEN delay_minutes > 5
        THEN 1 ELSE 0 END)       AS delayed_trips
FROM trips
GROUP BY is_peak_hour, hour_of_day
ORDER BY hour_of_day
"""
df3 = pd.read_sql(q3, engine)
df3.to_csv(f'{BASE}\\peak_analysis.csv', index=False)
print(f"✅ Peak analysis saved: {len(df3)} rows")
print(df3.head(5))

# ── Query 4: Stop Analysis ──
print("\nRunning Query 4: Stop Analysis...")
q4 = """
SELECT
    next_stop_name,
    route_id,
    COUNT(*)                     AS total_trips,
    ROUND(AVG(delay_minutes),2)  AS avg_delay,
    SUM(CASE WHEN delay_minutes > 5
        THEN 1 ELSE 0 END)       AS delayed_trips,
    ROUND(100.0 * SUM(CASE WHEN delay_minutes > 5
        THEN 1 ELSE 0 END)
        / COUNT(*), 1)           AS delay_rate_pct
FROM trips
WHERE next_stop_name IS NOT NULL
GROUP BY next_stop_name, route_id
HAVING total_trips > 50
ORDER BY avg_delay DESC
LIMIT 50
"""
df4 = pd.read_sql(q4, engine)
df4.to_csv(f'{BASE}\\stop_analysis.csv', index=False)
print(f"✅ Stop analysis saved: {len(df4)} rows")
print(df4.head(5))

print("\n🎉 ALL 4 QUERIES DONE!")
print("\nFiles saved:")
print("  ✅ delay_overview.csv")
print("  ✅ route_performance.csv")
print("  ✅ peak_analysis.csv")
print("  ✅ stop_analysis.csv")