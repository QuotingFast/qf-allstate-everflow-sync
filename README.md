# Quoting Fast Allstate allocated-spend mirror

This service mirrors newly authorized outbound Allstate clicks into the existing
Allstate advertiser report. It preserves each source record's original buyer
identifier, immutable allocated price, and occurrence time. It never opens a
traffic link, prepares an offer, submits a lead, or determines buyer acceptance.

An Everflow conversion created by this service means **allocated click spend
was mirrored**. It is not evidence of buyer acceptance, an approved payable
event, paid revenue, or settlement. Publisher attribution and partner economics
remain in the main application's separate ledger; this house-account mirror has
explicit zero partner payout and does not send publisher postbacks.

## Source and preserved fields

The only source is the authenticated read-only endpoint:
`POST https://lead-sms-api.onrender.com/api/admin/allstate/reporting-records`
(the known `https://q.autowiserate.com` origin is also allowed).

Request: `{"after_billing_id":"<exclusive cursor>","limit":100}`.
Response must contain `ok: true`, sorted `records`, `last_billing_id`, and a
boolean `has_more`. Source records must have:

- `record_kind: outbound_click_authorized` and
  `reporting_purpose: allocated_click_spend`;
- original `event_id`, `billing_id`, `buyer_id`, `internal_id`, `reporting_ref`;
- positive frozen `cost` as decimal text, at most four decimal places;
- valid `state`, boolean `homeowner`, and timezone-qualified `created_at`.
- `product: auto`, `destination_kind: buyer`, and no disagreement between a
  supplied `event_state` and the immutable billing `state`.

The mirror holds legacy rows, prepared offers, accepted leads, unknown prices,
missing identifiers/states, and records before the explicit activation time.
It does not infer missing values, reprice old rows, or read the legacy price CSV.
The original UTC fractional timestamp stays in `adv4` and the outbox; Everflow's
`date` field supports whole seconds. Eastern reporting days use the timezone
database, including daylight-saving changes. Homeowner describes an auto
consumer's homeownership, not a home insurance click.

Each conversion has one source event, a stable event-derived `order_id`, original
buyer ID in `adv1`, state in `adv2`, ownership in `adv3`, original timestamp in
`adv4`, and `allocated_click_spend` in `adv5`. Only base event `0` is supported.
Raw source contacts, customer URLs/tokens, source subIDs, and transaction IDs are
not stored or published by this mirror. They remain in the source ledger when
required for internal/publisher attribution.

## Durability and delivery verification

PostgreSQL persists the approved stream configuration, exclusive source cursor,
each normalized record, held exceptions, immutable payload, and delivery state.
Ingestion saves every row and advances the cursor in one transaction. That
cursor is an ingestion checkpoint, never a delivery acknowledgment. Changed
configuration, missing state, invalid pagination, or competing cursor updates
fail closed. No `/tmp` state file or implicit cursor-zero replay exists.

Delivery follows `ready → sending → received_unverified → confirmed`.
`sending` is committed before the API request. Exactly one worker can claim a
row. A timeout becomes `ambiguous`; a process crash leaves `sending`. Neither is
automatically submitted again, since the provider might already have created
the record. The stable order ID assists reconciliation but is not assumed to be
an Everflow idempotency guarantee.

Before creating anything, the service reads the provider's complete paginated
report for that event time and order ID. An exact existing row is not recreated.
After creation, an HTTP acknowledgment alone is insufficient: the service checks
the returned report's ID, offer, house affiliate, buyer ID, state, ownership,
original timestamp, currency, price, zero payout, and base-event status. Missing
readback remains unverified; conflicting or duplicate records are held. Even a
`confirmed` state means only this report match, never buyer acceptance.

Failures before a write can be retried normally. Once an attempt has been
committed, only read-only reconciliation runs automatically. If absence persists,
an operator must obtain conclusive provider evidence before any separately
reviewed replay. This code deliberately has no blanket reset/retry command.

## Configuration

All values below are explicit. There is no inferred affiliate, price, start time,
or start cursor. Never commit secrets or copy them into command output.

| Variable | Purpose |
|---|---|
| `EF_OFFER_ID` | Existing Allstate reporting offer: `4` |
| `EF_REPORTING_AFFILIATE_ID` | Existing internal reporting account: `3` |
| `EF_EVENT_ID` | `0` (base allocated-spend reporting record) |
| `EF_API_KEY` | Secret network API key; required only for provider read/publish |
| `EF_REPORTING_PURPOSE` | Exactly `allocated_click_spend` |
| `EF_PUBLISH` | `0` by default; publication additionally requires explicit `publish` or `run` |
| `MIRROR_DATABASE_URL` | Durable PostgreSQL DSN, authenticated TLS for remote hosts |
| `MIRROR_NOT_BEFORE` | Explicit UTC activation timestamp, saved with configuration |
| `REPORTING_ORIGIN` | `https://lead-sms-api.onrender.com` |
| `REPORTING_USER`, `REPORTING_PASSWORD` | Secret admin reader credentials |

Live read-only configuration verified on October 2, 2026: affiliate `3` is
`INTERNAL REV TRACK`, active, USD, and has no portal users. Offer `4` is public
with no hidden/rejected affiliate exceptions. The complete partner-postback
inventory had two entries, belonging to affiliates `4` and `6`, none to `3`.
Publication explicitly overrides payout to zero. Recheck this configuration
before activation; do not create a new account or send welcome email.

The original cron was in Oregon and the main database in Virginia. Use the
database's authenticated **external hostname**, not a cross-region private DNS
assumption. The connector forces `sslmode=verify-full` with system CAs and a
connection timeout. It does not disable certificate verification. Only local
temporary Unix sockets are allowed without TLS for synthetic tests.

## Release procedure

The supplied `render.yaml` starts in **preview only**, `EF_PUBLISH=0`, and disables
automatic deploys. Do not resume the old suspended code or turn traffic on to
test the release. Do not apply a blueprint without reviewing its service update.

1. Deploy and verify the main application's atomic click ledger and cursor-reader
   extension first. Keep Allstate's existing traffic pause and budgets unchanged.
2. Deploy this reviewed commit to the existing suspended service in preview mode.
   Add the explicit variables above. No new paid resource is required.
3. While traffic remains paused, read `COALESCE(MAX(id),0)` from `allstate_clicks`
   and the current UTC time in one read-only database transaction. Record those
   two values as the approved exclusive activation cursor and `MIRROR_NOT_BEFORE`.
   Account separately for known historical exceptions. Do not mark them fixed or
   delete them simply because this new stream is future-only.
4. Run the explicit preparation script once:

   ```sh
   python prepare_store.py --after-billing-id APPROVED_SOURCE_MAX_ID
   ```

   This applies only the two mirror tables/indexes and inserts a new stream in
   the existing database, in one transaction. Repeating initialization fails
   rather than resetting an existing cursor. It does not call Everflow.
5. Run `python sync.py preview`; verify the reader, TLS connection, preserved
   source/configuration, and aggregate-only output. An empty preview proves
   connectivity, not successful populated publication.
6. With the approved future-only reporting rollout, set `EF_PUBLISH=1` and change
   the start command to `python sync.py run` on this new commit. Only then resume
   this service. Keep the Allstate traffic pause until the broader restart
   checklist passes. The existing 15-minute schedule is reporting cadence,
   not a traffic enablement or a change to nightly buyer SFTP delivery.
7. On the first genuine future authorized source event, verify its one-to-one
   outbox and Everflow readback and confirm its ID, date, frozen price, and zero
   payout. Do not synthesize a billable click or publish a test conversion to
   manufacture this evidence. Buyer acceptance remains independently unverified.

Commands: `preview` (default, read only), `collect` (one source page into durable
outbox), `publish` (reconcile then at most 50 new submissions), `reconcile`
(read-only provider queries), `run` (one source page, reconciliation, at most 50
new submissions), `status`, and `initialize --after-billing-id N` (existing schema
only). A run stays bounded to 100 source rows, 50 previous uncertain rows, and 50
new submissions. Pending `ready` rows remain for later runs. Provider requests
are limited to two per second in each process; errors fail rather than blindly
repeating writes. Monitor backlog at the chosen cadence.

Output contains counts, cursor, and fixed diagnostic codes, not secrets or
consumer details. Held, ambiguous, conflicting, sending, or unverified records
set `requires_attention`; delivery commands exit `2` while any remain. Other
failures exit `1`. Configure normal failed-run monitoring before restart.

## Validation

```sh
python -m unittest -v test_sync
TEST_POSTGRES_DSN='host=/private/tmp/your-test-socket port=55467 dbname=postgres user=youruser' \
  python -m unittest -v test_postgres
```

The PostgreSQL suite refuses non-temporary-socket hosts and creates only a unique
synthetic `ef_test_*` schema, which it drops afterward. It tests actual concurrent
claims and source ingestion, cursor/row rollback, and reconnect recovery. Offline
tests prohibit network access. No production conversion, postback, lead, traffic
link, or customer-detail URL is used by tests.

The legacy `allstate_click_pricing_guide.csv` is retained as an unused historical
repository artifact. It cannot influence this service's event prices.

API contracts checked against Everflow's official documentation:
[manual reporting](https://developers.everflow.io/api-reference/post-networksconversionsreporting),
[report readback](https://developers.everflow.io/api-reference/post-networksreportingconversions),
[API quotas](https://developers.everflow.io/user-guide/rate-limiting).
