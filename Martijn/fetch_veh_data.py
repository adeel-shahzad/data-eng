#!/usr/bin/env python3
# rdw_lookup.py
# Quick demo: fetch vehicle data from NL RDW Open Data (Socrata/SODA)

import os, sys, re, json, time
from datetime import datetime
import requests

RDW_BASE_DATASET = "m9d7-ebf2"   # Gekentekende_voertuigen
RDW_FUEL_DATASET = "8ys7-d773"   # Gekentekende_voertuigen_brandstof
RDW_HOST = "https://opendata.rdw.nl"

NL_PLATE_RE = re.compile(r"^[A-Z0-9-]{5,8}$")

def nl_plate_normalize(text: str) -> str:
    return re.sub(r"\s+", "", text).upper()

def is_valid_nl_plate(plate: str) -> bool:
    return bool(NL_PLATE_RE.match(plate))

def rdw_get(dataset: str, plate: str, token: str | None):
    url = f"{RDW_HOST}/resource/{dataset}.json"
    headers = {"Accept": "application/json"}
    if token:
        headers["X-App-Token"] = token
    r = requests.get(url, headers=headers, params={"kenteken": plate}, timeout=10)
    r.raise_for_status()
    return r.json()

def parse_year(date_str: str | None) -> int | None:
    # RDW date format often "YYYYMMDD"
    if not date_str or len(date_str) < 4:
        return None
    try:
        return int(date_str[:4])
    except Exception:
        return None

def normalize(base: list[dict], fuel: list[dict]) -> dict:
    b0 = base[0] if base else {}
    f0 = fuel[0] if fuel else {}

    return {
        "make":            b0.get("merk"),
        "model":           b0.get("handelsbenaming"),
        "type":            b0.get("voertuigsoort"),     # e.g., BROMFIETS, MOTORFIETS
        "year":            parse_year(b0.get("datum_eerste_toelating")),
        "displacement_cc": int(b0["cilinderinhoud"]) if b0.get("cilinderinhoud") else None,
        "power_kw":        float(f0["nettomaximumvermogen"]) if f0.get("nettomaximumvermogen") else None,
        "fuel":            f0.get("brandstof_omschrijving")
    }

def main():
    if len(sys.argv) < 2:
        print("Usage: python rdw_lookup.py <NL_LICENSE_PLATE> [--save]")
        print("Optional env var: RDW_SODA_APP_TOKEN=<token>")
        sys.exit(1)

    plate = nl_plate_normalize(sys.argv[1])
    save_files = "--save" in sys.argv[2:]
    token = os.getenv("RDW_SODA_APP_TOKEN", "").strip() or None

    if not is_valid_nl_plate(plate):
        print(f"❌ Invalid NL plate format: {plate}")
        sys.exit(2)

    print(f"🔎 Looking up plate: {plate}")
    if token:
        print("🔐 Using SODA App Token from RDW_SODA_APP_TOKEN")
    else:
        print("ℹ️ No SODA token set (RDW_SODA_APP_TOKEN). It will still work, but rate limits are lower.")

    t0 = time.time()
    try:
        base = rdw_get(RDW_BASE_DATASET, plate, token)
        fuel = rdw_get(RDW_FUEL_DATASET, plate, token)
    except requests.HTTPError as e:
        print(f"❌ HTTP error: {e.response.status_code} {e.response.text[:300]}")
        sys.exit(3)
    except requests.RequestException as e:
        print(f"❌ Network error: {e}")
        sys.exit(4)

    dt = (time.time() - t0) * 1000.0
    if not base:
        print("⚠️ Not found in base dataset (m9d7-ebf2).")
    else:
        print(f"✅ RDW responded ({dt:.0f} ms).")

    norm = normalize(base, fuel)
    source_urls = {
        "base": f"{RDW_HOST}/resource/{RDW_BASE_DATASET}.json?kenteken={plate}",
        "fuel": f"{RDW_HOST}/resource/{RDW_FUEL_DATASET}.json?kenteken={plate}",
    }

    # Pretty console output for the client
    print("\n—— Normalized Vehicle Data —————————————————————")
    print(json.dumps(norm, indent=2, ensure_ascii=False))
    print("\n—— Source (RDW Open Data) ————————————————")
    print(f"Base: {source_urls['base']}")
    print(f"Fuel: {source_urls['fuel']}")

    if save_files:
        ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
        base_path = f"rdw_raw_base_{plate}_{ts}.json"
        fuel_path = f"rdw_raw_fuel_{plate}_{ts}.json"
        norm_path = f"rdw_normalized_{plate}_{ts}.json"
        with open(base_path, "w", encoding="utf-8") as f:
            json.dump(base, f, indent=2, ensure_ascii=False)
        with open(fuel_path, "w", encoding="utf-8") as f:
            json.dump(fuel, f, indent=2, ensure_ascii=False)
        with open(norm_path, "w", encoding="utf-8") as f:
            json.dump({
                "plate": plate,
                "source": "rdw",
                "latency_ms": round(dt),
                "normalized": norm,
                "source_urls": source_urls
            }, f, indent=2, ensure_ascii=False)
        print(f"\n💾 Saved files:\n  • {base_path}\n  • {fuel_path}\n  • {norm_path}")

if __name__ == "__main__":
    main()
