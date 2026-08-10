# qf-allstate-everflow-sync

**15-minute cron:** Fetches new Allstate ASC click events from Cloudflare D1 since last watermark and posts them as conversions to Everflow offer #4 (Allstate ASC - Auto Clicks).

## How It Works

1. Reads `watermark_id` from `/tmp/sync_state.json` (0 on first run)
2. Queries D1 `events` table for `allstate_redirect` events with `id > watermark_id`
3. Groups clicks by `(date_eastern, state)` 
4. Looks up revenue per state from `allstate_click_pricing_guide.csv`:
   - High-variance states (UT, ID, WA, WI, OH): `plus_30` price
   - TX, FL: `plus_20` price
   - All others: `plus_25` price
   - NY / unknown state: `$0.00`
5. Posts batches to Everflow via `POST /networks/conversions/reporting` (up to 50 per call)
6. Updates watermark to `max(id)` of processed rows

## Environment Variables

| Variable | Description |
|---|---|
| `CF_ACCOUNT_ID` | Cloudflare account ID |
| `CF_D1_DATABASE_ID` | D1 database ID for the events table |
| `CF_D1_API_TOKEN` | Cloudflare API token with D1 read access |
| `EF_API_KEY` | Everflow network API key |
| `EF_OFFER_ID` | Everflow offer ID (default: 4) |
| `EF_AFFILIATE_ID` | Everflow affiliate ID (default: 1) |
| `EF_TIMEZONE_ID` | Everflow timezone ID for conversions (default: 80 = Eastern) |
| `RATE_LIMIT` | Max Everflow API calls/second (default: 5) |
| `PRICING_CSV_PATH` | Path to the pricing CSV file |
| `DRY_RUN` | Set to "1" to skip actual API posts |

## D1 Schema

```sql
events (
  id INTEGER PRIMARY KEY,
  ts TEXT,         -- UTC ISO timestamp
  click_id TEXT,
  event TEXT,      -- 'allstate_redirect'
  meta TEXT,       -- JSON: {"state": "TX", "payout": 0, ...}
  ip TEXT,
  ...
)
```

## Deployment (Render)

1. Fork/use this repo on Render
2. Set `CF_D1_API_TOKEN` and `EF_API_KEY` as secret env vars in the Render dashboard
3. The `render.yaml` blueprint configures the cron schedule

## Notes

- Backfill for historical data (June 5 → Aug 2026) was run separately via `backfill.py`
- The watermark persists between runs on Render's ephemeral filesystem;
  for persistence, consider storing the watermark in Cloudflare KV or a DB
- Old repo `QuotingFast/qf-allstate-everflow-bridge` (disposition-based, DRY_RUN=1) 
  should be archived — it was never activated and uses a dead design
