# load_data.py — MTA Transport Delay Analysis FIXED

import pandas as pd
from sqlalchemy import create_engine
import warnings
warnings.filterwarnings('ignore')

MYSQL_PASSWORD = "chaitany41"

print("="*50)
print("STEP 1: Connecting to MySQL...")
engine = create_engine(
    f'mysql+pymysql://root:{MYSQL_PASSWORD}@localhost:3306/transport_analytics'
)
print("✅ Connected!")

print("\nSTEP 2: Reading CSV file...")
df = pd.read_csv(
    r'C:\Users\Mane Chaitanya\Desktop\transport-delay-project\data\raw\mta_1712.csv',
    encoding='utf-8',
    on_bad_lines='skip'
)
print(f"✅ Loaded {len(df):,} rows")

print("\nSTEP 3: Cleaning data...")

# ── Keep only columns we need ──
df = df[[
    'RecordedAtTime',
    'DirectionRef',
    'PublishedLineName',
    'OriginName',
    'NextStopPointName',
    'ArrivalProximityText',
    'DistanceFromStop',
    'ExpectedArrivalTime',
    'ScheduledArrivalTime'
]].copy()

# ── Rename columns ──
df.columns = [
    'recorded_time',
    'direction',
    'route_id',
    'origin_name',
    'next_stop_name',
    'arrival_proximity',
    'distance_from_stop',
    'expected_arrival',
    'scheduled_arrival_time'
]

# ── Parse RecordedAtTime as base date ──
df['recorded_time'] = pd.to_datetime(
    df['recorded_time'], errors='coerce'
)

# ── Parse ExpectedArrivalTime (full datetime) ──
df['expected_arrival'] = pd.to_datetime(
    df['expected_arrival'], errors='coerce'
)

# ── Fix ScheduledArrivalTime ──
# It's a time string like "24:01:35"
# We combine it with the recorded date

def fix_scheduled_time(row):
    try:
        base_date = row['recorded_time'].date()
        time_str  = str(row['scheduled_arrival_time']).strip()

        # Split HH:MM:SS
        parts = time_str.split(':')
        hour  = int(parts[0])
        mins  = int(parts[1])
        secs  = int(parts[2]) if len(parts) > 2 else 0

        # Handle hour >= 24 (next day)
        if hour >= 24:
            from datetime import timedelta, datetime
            base_dt = datetime.combine(base_date, 
                      pd.Timestamp('00:00:00').time())
            return base_dt + timedelta(
                hours=hour, minutes=mins, seconds=secs
            )

        return pd.Timestamp(
            f"{base_date} {hour:02d}:{mins:02d}:{secs:02d}"
        )
    except:
        return pd.NaT

print("  Fixing scheduled times (takes 1-2 mins)...")
df['scheduled_arrival'] = df.apply(
    fix_scheduled_time, axis=1
)

# ── Calculate delay in minutes ──
df['delay_minutes'] = (
    df['expected_arrival'] - df['scheduled_arrival']
).dt.total_seconds() / 60

# ── Extract time features ──
df['hour_of_day'] = df['recorded_time'].dt.hour
df['day_of_week'] = df['recorded_time'].dt.day_name()
df['month']       = df['recorded_time'].dt.month
df['date']        = df['recorded_time'].dt.date

# ── Flag peak hours ──
df['is_peak_hour'] = df['hour_of_day'].apply(
    lambda x: 1 if x in [7,8,9,17,18,19] else 0
)

# ── Remove bad rows ──
df = df.dropna(subset=['delay_minutes','route_id'])
df = df[df['delay_minutes'].between(-30, 120)]

print(f"✅ After cleaning: {len(df):,} rows")
print(f"\nSample delay stats:")
print(f"  Avg delay : {df['delay_minutes'].mean():.1f} mins")
print(f"  Max delay : {df['delay_minutes'].max():.1f} mins")
print(f"  Routes    : {df['route_id'].nunique()} unique")
print(f"  Date range: {df['date'].min()} to {df['date'].max()}")

print("\nSTEP 4: Loading into MySQL...")
# Select final columns for DB
final_cols = [
    'route_id', 'direction', 'origin_name',
    'next_stop_name', 'arrival_proximity',
    'distance_from_stop', 'expected_arrival',
    'scheduled_arrival', 'delay_minutes',
    'hour_of_day', 'day_of_week', 'month',
    'date', 'is_peak_hour'
]
df[final_cols].to_sql(
    'trips', engine,
    if_exists='replace',
    index=False,
    chunksize=500
)
print(f"✅ Loaded {len(df):,} rows into MySQL!")

# ── Save cleaned CSV ──
df[final_cols].to_csv(
    r'C:\Users\Mane Chaitanya\Desktop\transport-delay-project\data\processed\trips_clean.csv',
    index=False
)
print("✅ Clean CSV saved!")

print("\n" + "="*50)
print("🎉 ALL DATA LOADED SUCCESSFULLY!")
print("="*50)