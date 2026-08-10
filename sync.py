#!/usr/bin/env python3
"""
qf-allstate-everflow-sync
Runs every 15 minutes (Render cron).
Fetches new allstate_redirect events from D1 since last watermark,
groups by (date_eastern, state), posts conversions to Everflow offer #4.
Watermark is stored in a small state file (KV-style JSON).
"""
import json, os, time, urllib.request, urllib.error, csv, io
from datetime import datetime, timezone, timedelta
from collections import defaultdict
from pathlib import Path

# ── Config (from env vars) ────────────────────────────────────────────────────
CF_ACCOUNT  = os.environ["CF_ACCOUNT_ID"]
CF_DB       = os.environ["CF_D1_DATABASE_ID"]
CF_TOKEN    = os.environ["CF_D1_API_TOKEN"]
EF_API_KEY  = os.environ["EF_API_KEY"]
EF_OFFER    = int(os.getenv("EF_OFFER_ID", "4"))
EF_AFFILIATE= int(os.getenv("EF_AFFILIATE_ID", "1"))
EF_TZ_ID    = int(os.getenv("EF_TIMEZONE_ID", "80"))  # America/New_York
EF_MAX_BATCH= 50  # Everflow max number_of_conversions per call
RATE_LIMIT  = float(os.getenv("RATE_LIMIT", "5"))     # API calls/sec

PRICING_CSV = os.getenv("PRICING_CSV_PATH", "allstate_click_pricing_guide.csv")
STATE_FILE  = os.getenv("STATE_FILE", "/tmp/sync_state.json")
DRY_RUN     = os.getenv("DRY_RUN", "0") == "1"

CF_D1_URL   = (f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT}"
               f"/d1/database/{CF_DB}/query")
EF_CONV_URL = "https://api.eflow.team/v1/networks/conversions/reporting"
EASTERN     = timezone(timedelta(hours=-4))   # EDT (adjust for EST in winter if needed)

HIGH_VARIANCE = {"UT", "ID", "WA", "WI", "OH"}
TX_FL         = {"TX", "FL"}


def load_pricing():
    pricing = {}
    with open(PRICING_CSV) as f:
        for row in csv.DictReader(f):
            s = row["State"]
            pricing[s] = {
                "plus_20": float(row["Allstate_Price_20pct"]),
                "plus_25": float(row["Allstate_Price_25pct"]),
                "plus_30": float(row["Allstate_Price_30pct"]),
            }
    return pricing


def revenue_for_state(state, pricing):
    if not state or state not in pricing:
        return 0.0
    p = pricing[state]
    if state in HIGH_VARIANCE: return p["plus_30"]
    if state in TX_FL:         return p["plus_20"]
    return p["plus_25"]


def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {"watermark_id": 0, "watermark_ts": ""}


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)


def d1_query(sql):
    body = json.dumps({"sql": sql}).encode()
    req = urllib.request.Request(
        CF_D1_URL, data=body,
        headers={"Authorization": f"Bearer {CF_TOKEN}",
                 "Content-Type": "application/json"},
        method="POST"
    )
    with urllib.request.urlopen(req, timeout=60) as r:
        d = json.loads(r.read())
    if not d.get("success"):
        raise RuntimeError(f"D1 error: {d.get('errors')}")
    return d["result"][0]["results"]


def ef_post(date_str, revenue, count):
    """Post `count` identical conversions to Everflow."""
    payload = {
        "offer_id":              EF_OFFER,
        "affiliate_id":          EF_AFFILIATE,
        "event_id":              0,
        "number_of_conversions": count,
        "timezone_id":           EF_TZ_ID,
        "is_now":                False,
        "date":                  date_str,
        "revenue_amount":        revenue,
        "is_revenue_amount_submitted": True,
        "payout_amount":         0,
        "is_payout_amount_submitted":  True,
        "internal_notes":        f"sync {date_str} n={count}",
    }
    if DRY_RUN:
        print(f"  DRY: {date_str} n={count} rev=${revenue:.2f}")
        return True
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        EF_CONV_URL, data=body,
        headers={"X-Eflow-API-Key": EF_API_KEY,
                 "Content-Type": "application/json"},
        method="POST"
    )
    for attempt in range(3):
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return json.loads(r.read()).get("result", False)
        except urllib.error.HTTPError as e:
            code = e.code
            print(f"  HTTP {code} attempt {attempt+1}")
            if code == 429 or code >= 500:
                time.sleep(2 ** attempt)
            else:
                return False
        except Exception as ex:
            print(f"  Error: {ex}")
            time.sleep(2 ** attempt)
    return False


def main():
    print(f"[{datetime.now(timezone.utc).isoformat()}] Starting sync")
    pricing  = load_pricing()
    state    = load_state()
    wm_id    = state.get("watermark_id", 0)
    throttle = 1.0 / RATE_LIMIT

    # Fetch new clicks since watermark
    rows = d1_query(
        f'SELECT id, ts, click_id, meta '
        f'FROM events WHERE event="allstate_redirect" AND id > {wm_id} '
        f'ORDER BY id ASC LIMIT 5000'
    )
    if not rows:
        print("  No new clicks. Done.")
        return

    print(f"  {len(rows)} new clicks since id={wm_id}")

    # Group by (date_eastern, state)
    groups = defaultdict(lambda: defaultdict(int))  # date_e → state → count
    max_id = wm_id
    for row in rows:
        d1_id  = row["id"]
        ts_utc = row["ts"]
        try:
            meta = json.loads(row.get("meta") or "{}")
        except Exception:
            meta = {}
        state_val = meta.get("state") or ""
        try:
            dt_e = datetime.fromisoformat(
                ts_utc.rstrip("Z") + "+00:00"
            ).astimezone(EASTERN)
            date_e   = dt_e.strftime("%Y-%m-%d")
            time_e   = dt_e.strftime("%H:%M:%S")
        except Exception:
            date_e = ts_utc[:10]
            time_e = "12:00:00"
        groups[date_e][state_val] += 1
        if d1_id > max_id:
            max_id = d1_id

    total_clicks = sum(sum(sv.values()) for sv in groups.values())
    total_groups = sum(len(sv) for sv in groups.values())
    print(f"  {total_clicks} clicks → {total_groups} groups")

    # Post to Everflow
    posted_clicks = 0
    errors = 0
    total_revenue = 0.0
    for date_e, state_map in sorted(groups.items()):
        for st, cnt in sorted(state_map.items()):
            rev = revenue_for_state(st, pricing)
            date_str = f"{date_e} 12:00:00"
            for i in range(0, cnt, EF_MAX_BATCH):
                chunk_n = min(cnt - i, EF_MAX_BATCH)
                ok = ef_post(date_str, rev, chunk_n)
                if ok:
                    posted_clicks += chunk_n
                    total_revenue += rev * chunk_n
                else:
                    errors += 1
                time.sleep(throttle)

    # Update watermark
    state["watermark_id"] = max_id
    state["watermark_ts"] = datetime.now(timezone.utc).isoformat()
    save_state(state)

    print(f"  Posted {posted_clicks} clicks | ${total_revenue:.2f} revenue | {errors} errors")
    print(f"  New watermark: id={max_id}")


if __name__ == "__main__":
    main()
