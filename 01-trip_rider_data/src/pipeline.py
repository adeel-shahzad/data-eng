
import argparse, json
from pathlib import Path
import pandas as pd

def load_riders(path: Path) -> pd.DataFrame:
    return pd.read_json(path, lines=True)

def load_trips(input_dir: Path, watermark_date: str) -> pd.DataFrame:
    files = [f for f in input_dir.glob("trips_*.csv") if f.stem.split("_")[-1] <= watermark_date]
    if not files:
        print("No trip files found")
        return pd.DataFrame()
    else:
        print(f"Loading {len(files)} trip files")
    dfs = [pd.read_csv(f) for f in files]
    df = pd.concat(dfs, ignore_index=True)
    df["fare"] = pd.to_numeric(df["fare"], errors="coerce")
    df = df.dropna(subset=["fare"])
    df["event_time"] = pd.to_datetime(df["event_time"], utc=True, errors="coerce")
    df = df.dropna(subset=["event_time"])
    return df

def dedupe_latest(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values("event_time").drop_duplicates("trip_id", keep="last")

def join_with_riders(trips: pd.DataFrame, riders: pd.DataFrame) -> pd.DataFrame:
    return trips.merge(riders, on="rider_id", how="left").fillna({"country": "UNK"})

def aggregate_daily(trips: pd.DataFrame) -> pd.DataFrame:
    daily = trips.groupby("ingestion_date").agg(
        total_trips=("trip_id","count"),
        completed_trips=("status", lambda x: (x=="completed").sum()),
        avg_fare=("fare","mean")
    ).reset_index().rename(columns={"ingestion_date":"date"})
    daily["avg_fare"] = daily["avg_fare"].round(2)
    return daily

def aggregate_daily_country(trips: pd.DataFrame) -> pd.DataFrame:
    daily_country = trips.groupby(["ingestion_date","country"]).agg(
        trips=("trip_id","count"),
        gmv=("fare","sum")
    ).reset_index().rename(columns={"ingestion_date":"date"})
    daily_country["gmv"] = daily_country["gmv"].round(2)
    return daily_country

def run(input_dir: Path, dim_path: Path, out_dir: Path, watermark_date: str):
    riders = load_riders(dim_path)
    trips = load_trips(input_dir, watermark_date)
    if trips.empty:
        return {"status":"no_data"}
    trips_latest = dedupe_latest(trips)
    trips_joined = join_with_riders(trips_latest, riders)
    out_dir.mkdir(parents=True, exist_ok=True)
    facts_dir = out_dir / f"facts/date={watermark_date}"
    facts_dir.mkdir(parents=True, exist_ok=True)
    trips_joined.to_csv(facts_dir/"trips_latest.csv", index=False)
    aggregate_daily(trips_joined).to_csv(out_dir/"daily.csv", index=False)
    aggregate_daily_country(trips_joined).to_csv(out_dir/"daily_by_country.csv", index=False)
    return {"status":"ok","facts":str(facts_dir),"aggs":str(out_dir)}

def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--input", type=Path, required=True)
    p.add_argument("--dim", type=Path, required=True)
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--date", type=str, required=True)
    args = p.parse_args(argv)
    res = run(args.input, args.dim, args.out, args.date)
    print(json.dumps(res, indent=2))

if __name__ == "__main__":
    main()
