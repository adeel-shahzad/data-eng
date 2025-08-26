
# Data Engineering Coding Round — Pandas Version

Run:
```bash
python -m src.pipeline_pandas \
  --input data/input \
  --dim data/dim/riders.jsonl \
  --out data/out \
  --date 2025-08-18
```
Key steps: load → validate → dedupe latest per `trip_id` → join riders → write partitioned facts + daily aggregates.
