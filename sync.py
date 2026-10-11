#!/usr/bin/env python3
"""Quoting Fast actual-click spend mirror. Default execution is read-only preview.

No prepared redirects, historical backfill, state-price estimation, fake tracking
IDs or inferred buyer acceptance. PostgreSQL owns all cursor/delivery state.
"""
from __future__ import annotations
import argparse
import base64
import hashlib
import json
import math
import os
import re
import ssl
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

UTC = timezone.utc
EASTERN = ZoneInfo("America/New_York")
UTC_TIMEZONE_ID = 67  # /meta/timezones verified 2026-10-02, timezone=UTC
ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{5,127}\Z")
STATES = set("AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY DC".split())
KIND, PURPOSE = "outbound_click_authorized", "allocated_click_spend"


class Held(ValueError):
    """Fixed diagnostic codes only; never consumer data."""


def dump(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(value):
    return hashlib.sha256(dump(value).encode()).hexdigest()


def instant(value):
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if result.tzinfo is None:
            raise ValueError()
        return result.astimezone(UTC)
    except (ValueError, TypeError, AttributeError):
        raise Held("invalid_explicit_timestamp") from None


def identifier(value, field):
    if not isinstance(value, str) or not ID.fullmatch(value):
        raise Held("invalid_" + field)
    return value


def billing_id(value):
    if isinstance(value, bool) or not re.fullmatch(r"[1-9][0-9]{0,17}", str(value)):
        raise Held("invalid_billing_id")
    return int(value)


@dataclass(frozen=True)
class Config:
    offer_id: int
    affiliate_id: int
    event_id: int
    not_before: str
    stream: str = "allstate-actual-v1"

    def __post_init__(self):
        if self.offer_id <= 0 or self.affiliate_id <= 0 or self.event_id != 0:
            raise ValueError("explicit_reporting_identity_required")
        instant(self.not_before)
        identifier(self.stream, "stream")


def normalize(row, cfg, now=None):
    """Strict projection: never retain contacts, customer URLs, tokens or payloads."""
    bid = billing_id(row.get("billing_id"))
    if row.get("record_kind") != KIND or row.get("reporting_purpose") != PURPOSE:
        raise Held("not_an_authorized_outbound_spend_record")
    if row.get("product") != "auto" or row.get("destination_kind") != "buyer":
        raise Held("not_an_auto_buyer_destination")
    event = identifier(row.get("event_id"), "event_id")
    buyer = identifier(row.get("buyer_id"), "buyer_id")
    internal = identifier(row.get("internal_id"), "internal_id")
    ref = identifier(row.get("reporting_ref"), "reporting_ref")
    if row.get("state") not in STATES:
        raise Held("missing_or_invalid_state")
    if row.get("event_state") is not None and row["event_state"] != row["state"]:
        raise Held("event_state_conflict")
    if type(row.get("homeowner")) is not bool:
        raise Held("missing_or_invalid_homeowner")
    try:
        if not isinstance(row.get("cost"), str) or not re.fullmatch(r"[0-9]+(?:\.[0-9]{1,4})?", row["cost"]):
            raise InvalidOperation()
        cost = Decimal(row["cost"])
        if not cost.is_finite() or cost <= 0 or not math.isfinite(float(cost)) or Decimal(str(float(cost))) != cost:
            raise InvalidOperation()
    except (InvalidOperation, ValueError):
        raise Held("missing_or_invalid_immutable_cost") from None
    occurred = instant(row.get("created_at"))
    if occurred < instant(cfg.not_before):
        raise Held("before_approved_activation")
    if occurred > (now or datetime.now(UTC)) + timedelta(minutes=5):
        raise Held("future_event")
    return {"billing_id":bid,"event_id":event,"record_kind":KIND,
            "buyer_id":buyer,"internal_id":internal,"reporting_ref":ref,
            "state":row["state"],"homeowner":row["homeowner"],"cost":str(cost),
            "created_at":occurred.isoformat(timespec="microseconds" if occurred.microsecond%1000 else "milliseconds").replace("+00:00","Z"),
            "eastern_day":occurred.astimezone(EASTERN).date().isoformat(),
            "purpose":PURPOSE,"buyer_acceptance":"unconfirmed","settlement":"unconfirmed"}


def payload(record, cfg):
    return {"offer_id":cfg.offer_id,"affiliate_id":cfg.affiliate_id,"event_id":cfg.event_id,
            "number_of_conversions":1,"timezone_id":UTC_TIMEZONE_ID,"is_now":False,
            # Provider supports seconds. Original fractional time remains in adv4/outbox.
            "date":instant(record["created_at"]).strftime("%Y-%m-%d %H:%M:%S"),
            "order_id":"qfas_"+hashlib.sha256(record["event_id"].encode()).hexdigest(),
            "adv1":record["buyer_id"],"adv2":record["state"],
            "adv3":"Homeowner" if record["homeowner"] else "NonHomeowner",
            "adv4":record["created_at"],"adv5":PURPOSE,
            "revenue_amount":float(Decimal(record["cost"])),"is_revenue_amount_submitted":True,
            "payout_amount":0,"is_payout_amount_submitted":True,
            "internal_notes":"Quoting Fast allocated click spend; buyer acceptance and settlement unconfirmed."}


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        raise RuntimeError("unexpected_api_redirect")


def json_request(url, body, headers):
    req = urllib.request.Request(url,data=dump(body).encode(),
          headers={"Content-Type":"application/json",**headers},method="POST")
    # Never follow a redirect to a customer destination or forward credentials.
    with urllib.request.build_opener(NoRedirect()).open(req,timeout=20) as response:
        return json.load(response)


class Source:
    def __init__(self, origin, user, password):
        if origin not in {"https://lead-sms-api.onrender.com","https://q.autowiserate.com"}:
            raise ValueError("unapproved_source_origin")
        self.url = origin+"/api/admin/allstate/reporting-records"
        self.headers = {"Authorization":"Basic "+base64.b64encode((user+":"+password).encode()).decode()}

    def page(self, after, limit=100):
        result = json_request(self.url,{"after_billing_id":str(after),"limit":limit},self.headers)
        if result.get("ok") is not True or not isinstance(result.get("records"),list):
            raise RuntimeError("source_read_failed")
        return result


class Everflow:
    def __init__(self, key):
        self.headers = {"X-Eflow-Api-Key":key}
        self.base = "https://api.eflow.team/v1"
        self.next_request = 0.0

    def request(self, path, body):
        # Leave headroom in the network-wide API quota. Never retry a write here.
        wait = self.next_request-time.monotonic()
        if wait>0:
            time.sleep(wait)
        self.next_request=time.monotonic()+0.5
        return json_request(self.base+path,body,self.headers)

    def create(self, body):
        # One attempt only: an exception can follow a successfully created record.
        result = self.request("/networks/conversions/reporting",body)
        if result.get("result") is not True:
            raise RuntimeError("provider_result_not_confirmed")

    def lookup(self, body):
        # No assumption about undocumented order-id filtering or idempotency.
        dt = datetime.strptime(body["date"],"%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        day = dt.replace(hour=0,minute=0,second=0,microsecond=0)
        # Manual reporting can normalize the provider timestamp to midnight.
        # The immutable original time is still required in adv4 on readback.
        query = {"from":day.strftime("%Y-%m-%d %H:%M:%S"),
                 "to":(day+timedelta(days=1)-timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S"),
                 "timezone_id":UTC_TIMEZONE_ID,"currency_id":"USD",
                 "show_conversions":True,"show_events":True,
                 "query":{"filters":[{"resource_type":"offer","filter_id_value":str(body["offer_id"])}]}}
        matches=[];total=None
        for page in range(1,11):
            data=self.request(f"/networks/reporting/conversions?page={page}&page_size=100",query)
            rows,paging=data.get("conversions"),data.get("paging")
            if not isinstance(rows,list) or not isinstance(paging,dict):
                raise RuntimeError("incomplete_provider_readback")
            count=paging.get("total_count");size=paging.get("page_size")
            if (type(paging.get("page")) is not int or paging["page"]!=page
                    or type(size) is not int or type(count) is not int or count<0):
                raise RuntimeError("invalid_provider_pagination")
            # Everflow reports page_size=1 for an empty result even when 100
            # was requested. A shorter first page is complete only when its
            # rows account for the entire declared result, including zero.
            if any(not isinstance(r,dict) for r in rows):
                raise RuntimeError("incomplete_provider_readback")
            if page==1 and count<=100 and len(rows)==count and max(1,count)<=size<=100:
                return [r for r in rows if provider_order_matches(body,r)]
            if size!=100 or (total is not None and count!=total):
                raise RuntimeError("invalid_provider_pagination")
            total=count
            expected=min(100,max(0,total-(page-1)*100))
            if len(rows)!=expected or not rows:
                raise RuntimeError("truncated_provider_readback")
            matches.extend(r for r in rows if provider_order_matches(body,r))
            if page*100>=total:
                return matches
        raise RuntimeError("provider_readback_limit")


def provider_order_matches(body, row):
    # This endpoint retains at most 50 order-id characters. A canonical match
    # only selects candidates; full immutable identity is checked below.
    order=body["order_id"]
    return row.get("order_id") in {order,order[:50]}


def confirmed_match(body, matches):
    if len(matches)!=1:
        return None
    row=matches[0];rel=row.get("relationship") or {}
    try:
        same=provider_order_matches(body,row)
        same=same and all(row.get(k)==body[k] for k in ["adv1","adv2","adv3","adv4","adv5"])
        same=same and rel.get("offer",{}).get("network_offer_id")==body["offer_id"]
        same=same and rel.get("affiliate",{}).get("network_affiliate_id")==body["affiliate_id"]
        same=same and Decimal(str(row["revenue"]))==Decimal(str(body["revenue_amount"])) and Decimal(str(row["payout"]))==0
        same=same and row.get("currency_id")=="USD" and row.get("status")=="approved"
        dt=datetime.strptime(body["date"],"%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        stamp=int(dt.timestamp());day_stamp=int(dt.replace(hour=0,minute=0,second=0,microsecond=0).timestamp())
        # Accept only the exact timestamp or the observed midnight projection,
        # never an arbitrary time on the same day. adv4 above remains exact.
        same=same and type(row.get("conversion_unix_timestamp")) is int and row["conversion_unix_timestamp"] in {stamp,day_stamp}
        same=same and body["event_id"]==0 and row.get("is_event") is False
        cv=row.get("conversion_id")
        return cv if same and isinstance(cv,str) and cv else None
    except (KeyError,InvalidOperation,TypeError,ValueError):
        return None


def reconcile_one(store, provider, bid, body):
    matches=provider.lookup(body)
    match=confirmed_match(body,matches)
    if match:
        store.mark(bid,"confirmed",conversion_id=match)
        return "confirmed"
    if matches:
        store.mark(bid,"conflict","provider_record_mismatch_or_duplicate")
        return "conflict"
    # Absence is not proof of noncreation: reporting can lag. Never auto-retry.
    return "unverified"


def publish_ready(store, provider, limit=50):
    for bid,encoded,state in store.candidates(["ready"],limit):
        body=json.loads(encoded)
        result=reconcile_one(store,provider,bid,body)
        if result!="unverified" or not store.claim(bid):
            continue
        try:
            provider.create(body)
        except Exception:
            store.mark(bid,"ambiguous","submission_outcome_requires_readback")
            continue
        store.mark(bid,"received_unverified","http_ack_is_not_report_readback")
        reconcile_one(store,provider,bid,body)


def database_options(dsn):
    """Production connections must authenticate the database server over TLS."""
    from psycopg.conninfo import conninfo_to_dict
    options=conninfo_to_dict(dsn)
    host=options.get("host","")
    if host in {"/tmp","/private/tmp"} or host.startswith(("/tmp/","/private/tmp/")):
        return {"autocommit":True}  # Synthetic local integration cluster only.
    if not host or host.startswith("/") or "," in host:
        raise RuntimeError("explicit_verified_database_host_required")
    # Binary libpq can search a different CA directory than the host Python.
    # Use the host's trusted CA bundle when available; never disable validation.
    ca_file=ssl.get_default_verify_paths().cafile or "system"
    return {"autocommit":True,"sslmode":"verify-full","sslrootcert":ca_file,"connect_timeout":10}


def main(argv=None):
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command",choices=["preview","initialize","collect","publish","reconcile","run","status"],nargs="?",default="preview")
    parser.add_argument("--after-billing-id",type=int)
    args=parser.parse_args(argv)
    cfg=Config(int(os.environ["EF_OFFER_ID"]),int(os.environ["EF_REPORTING_AFFILIATE_ID"]),int(os.environ["EF_EVENT_ID"]),os.environ["MIRROR_NOT_BEFORE"])
    import psycopg
    from store import Store
    if args.command in {"publish","run"} and (os.environ.get("EF_PUBLISH")!="1" or os.environ.get("EF_REPORTING_PURPOSE")!=PURPOSE):
        raise RuntimeError("publication_not_enabled")
    dsn=os.environ["MIRROR_DATABASE_URL"]
    with psycopg.connect(dsn,**database_options(dsn)) as db:
        db.execute("SET statement_timeout='10s'")
        store=Store(db,cfg)
        if args.command=="initialize":
            if args.after_billing_id is None or args.after_billing_id<0:
                raise RuntimeError("explicit_activation_cursor_required")
            store.initialize(args.after_billing_id)
        elif args.command in {"preview","collect","run"}:
            source=Source(os.environ["REPORTING_ORIGIN"],os.environ["REPORTING_USER"],os.environ["REPORTING_PASSWORD"])
            after=store.cursor();page=source.page(after)
            if args.command in {"collect","run"}:
                store.ingest(after,page)
            else:
                reasons={};eligible=0;total=Decimal(0)
                for row in page["records"]:
                    try:
                        r=normalize(row,cfg);eligible+=1;total+=Decimal(r["cost"])
                    except Held as e:
                        reasons[str(e)]=reasons.get(str(e),0)+1
                print(dump({"mode":"preview_no_writes","rows":len(page["records"]),"eligible":eligible,"allocated_click_spend":str(total),"held":reasons,"has_more":page.get("has_more")}))
        if args.command in {"publish","reconcile","run"}:
            store.cursor()  # Confirm immutable stream configuration before any API call.
            provider=Everflow(os.environ["EF_API_KEY"])
            if args.command in {"publish","run"}:
                if os.environ.get("EF_PUBLISH")!="1" or os.environ.get("EF_REPORTING_PURPOSE")!=PURPOSE:
                    raise RuntimeError("publication_not_enabled")
            for bid,encoded,state in store.candidates(["sending","ambiguous","received_unverified"],50):
                reconcile_one(store,provider,bid,json.loads(encoded))
            if args.command in {"publish","run"}:
                publish_ready(store,provider)
        summary=store.summary()
        attention=any(summary.get(s,0) for s in ["held","ambiguous","conflict","sending","received_unverified"])
        print(dump({"cursor":store.cursor(),"outbox":summary,"requires_attention":attention}))
        if attention and args.command in {"run","publish","reconcile"}:
            raise SystemExit(2)


if __name__=="__main__":
    try:
        main()
    except Exception as exc:
        print(dump({"ok":False,"error_type":type(exc).__name__}),file=sys.stderr)
        raise SystemExit(1)
