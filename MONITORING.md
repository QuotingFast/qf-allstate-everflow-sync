# Operating and monitoring notes

## Do not break the start-command guard

The Render start command asserts the SHA-256 of `sync.py` before importing it
("legacy build disabled"). Any merged change to `sync.py` must be deployed
together with an updated hash in the Render start command, or every run fails
with an `AssertionError`. Changes to other files do not affect the guard.

Auto-deploy is off. A merge to `main` does nothing until someone deploys it.

## Runtime pins

- `psycopg[binary]==3.3.6` in `requirements.txt`.
- Render env `PYTHON_VERSION=3.14.3` (the version production already ran),
  so a Render default-runtime change cannot silently alter the job.
- `main` requires the `regression` CI check before a merge.

## Exit codes

- `0`: run finished and nothing needs attention.
- `1`: an exception (traceback in the run log; for example
  `invalid_provider_pagination`). Nothing new was submitted after the error.
- `2`: the run finished, but the outbox has rows that need attention
  (`held`, `ambiguous`, `conflict`, `sending`, `received_unverified`).
  The final JSON log line shows the counts.

Every run prints one summary line:
`{"cursor":N,"outbox":{"confirmed":N,...},"requires_attention":false}`

## Hourly health check

1. Render: `GET https://api.render.com/v1/services/crn-d9skhvfavr4c73bf3b50`.
   Alert if `suspended != "not_suspended"`, or if
   `serviceDetails.lastSuccessfulRunAt` is older than 45 minutes
   (the schedule is every 15 minutes).
2. Render: `GET /v1/services/crn-d9skhvfavr4c73bf3b50/events?limit=20`.
   Alert if the most recent `cron_job_run_ended` has
   `details.status == "unsuccessful"`, and include
   `details.reason.nonZeroExit`.
3. Everflow: `POST https://api.eflow.team/v1/networks/reporting/conversions?page=1&page_size=100`
   with offer filter `4`, `timezone_id` 67 and today's UTC window. The count of
   rows with `adv5 == "allocated_click_spend"` for affiliate `3` should match
   the `confirmed` delta in the summary log line. Alert on duplicate `order_id`
   values or on any row whose status is not `approved`.
