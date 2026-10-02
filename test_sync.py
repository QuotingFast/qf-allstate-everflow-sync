"""Offline tests: synthetic identities, local SQLite SQL harness, zero network.

The SQL harness exercises persistence, uniqueness and rollback. PostgreSQL's row
locking additionally needs the optional TEST_POSTGRES_DSN integration suite.
"""
import copy
import json
import os
import sqlite3
import tempfile
import unittest
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import sync
from store import Store

NOW=datetime(2027,1,1,tzinfo=timezone.utc)
CFG=sync.Config(4,3,0,"2026-10-01T00:00:00Z")


def row(bid=101,**kw):
    return {"billing_id":str(bid),"event_id":f"synthetic-event-{bid}",
      "record_kind":sync.KIND,"reporting_purpose":sync.PURPOSE,
      "buyer_id":f"synthetic-buyer-{bid}","internal_id":f"synthetic-internal-{bid}",
      "reporting_ref":f"synthetic-report-{bid}","cost":"8.3250","state":"FL",
      "homeowner":False,"created_at":"2026-10-01T05:06:07.123Z",**kw}


def page(*rows):
    return {"ok":True,"records":list(rows),"last_billing_id":rows[-1]["billing_id"] if rows else "100","has_more":False}


class SQLiteHarness:
    """No production fallback. Test only translation of DB-API parameters/locks."""
    def __init__(self,path):
        self.db=sqlite3.connect(path,isolation_level=None)
        self.db.executescript(Path(__file__).with_name("schema.sql").read_text())

    @contextmanager
    def transaction(self):
        self.db.execute("BEGIN")
        try:
            yield
            self.db.execute("COMMIT")
        except BaseException:
            self.db.execute("ROLLBACK")
            raise

    def execute(self,sql,params=()):
        sql=sql.replace(" FOR UPDATE","")
        if "state=ANY(%s)" in sql:
            sql=sql.replace("state=ANY(%s)","state IN ("+",".join("%s" for _ in params[1])+")")
            params=(params[0],*params[1],params[2])
        return self.db.execute(sql.replace("%s","?"),params)


class FakeProvider:
    def __init__(self,fail=False,delay=False):
        self.creates=[];self.records=[];self.fail=fail;self.delay=delay

    def lookup(self,body):
        return copy.deepcopy(self.records)

    def create(self,body):
        self.creates.append(copy.deepcopy(body))
        if self.fail:
            raise TimeoutError()
        if not self.delay:
            self.records=[receipt(body)]


def receipt(body,**kw):
    return {**{k:body[k] for k in ["order_id","adv1","adv2","adv3","adv4","adv5"]},
       "conversion_id":"synthetic-conversion-1","revenue":body["revenue_amount"],"payout":0,
       "currency_id":"USD","status":"approved","is_event":False,
       "conversion_unix_timestamp":int(sync.instant(body["date"].replace(" ","T")+"Z").timestamp()),
       "relationship":{"offer":{"network_offer_id":4},"affiliate":{"network_affiliate_id":3}},**kw}


class Tests(unittest.TestCase):
    def setUp(self):
        self.net=patch("urllib.request.OpenerDirector.open",side_effect=AssertionError("network forbidden in tests"));self.net.start()
        self.tmp=tempfile.TemporaryDirectory();self.path=str(Path(self.tmp.name)/"outbox.db")
        self.db=SQLiteHarness(self.path);self.store=Store(self.db,CFG);self.store.initialize(100)

    def tearDown(self):
        self.db.db.close();self.tmp.cleanup();self.net.stop()

    def body(self):
        return sync.payload(sync.normalize(row(),CFG,NOW),CFG)

    def ingest(self,*rows):
        self.store.ingest(self.store.cursor(),page(*(rows or [row()])))

    def test_preserves_frozen_cost_original_ids_and_milliseconds(self):
        record=sync.normalize(row(),CFG,NOW);body=sync.payload(record,CFG)
        self.assertEqual(record["cost"],"8.3250")
        self.assertEqual(body["revenue_amount"],8.325)
        self.assertEqual(body["adv1"],row()["buyer_id"])
        self.assertEqual(body["adv4"],row()["created_at"])
        self.assertEqual(body["date"],"2026-10-01 05:06:07")
        self.assertEqual(body["payout_amount"],0)
        self.assertEqual(body["number_of_conversions"],1)
        self.assertEqual(record["settlement"],"unconfirmed")

    def test_prepared_offer_lead_acceptance_and_legacy_are_held(self):
        for kind in [None,"allstate_redirect","shopnbuy","accepted_lead","offer_render"]:
            with self.subTest(kind=kind),self.assertRaises(sync.Held):
                sync.normalize(row(record_kind=kind),CFG,NOW)

    def test_missing_fields_are_not_invented(self):
        for kw in [{"state":""},{"cost":None},{"cost":8.0},{"cost":"NaN"},{"cost":"0"},{"cost":"-2"},{"cost":"1e309"},{"cost":"99999999999999999999999999999999999.9999"},{"homeowner":None},{"homeowner":"false"},{"buyer_id":""},{"event_id":""},{"created_at":"2026-10-01"},{"reporting_purpose":"buyer_paid"}]:
            with self.subTest(kw=kw),self.assertRaises(sync.Held):
                sync.normalize(row(**kw),CFG,NOW)

    def test_submillisecond_source_time_is_not_truncated(self):
        record=sync.normalize(row(created_at="2026-10-01T05:06:07.123456Z"),CFG,NOW)
        self.assertEqual(sync.payload(record,CFG)["adv4"],"2026-10-01T05:06:07.123456Z")

    def test_historical_allocation_cannot_be_repriced_or_replayed(self):
        with self.assertRaisesRegex(sync.Held,"before_approved_activation"):
            sync.normalize(row(created_at="2026-07-10T12:00:00Z",cost="2.6000"),CFG,NOW)

    def test_eastern_midnight_and_dst_preserve_absolute_time(self):
        cases=[("2026-11-02T04:30:00.000Z","2026-11-01"),
               ("2026-11-01T05:30:00.000Z","2026-11-01"),
               ("2026-11-01T06:30:00.000Z","2026-11-01")]
        dates=[]
        for stamp,day in cases:
            r=sync.normalize(row(created_at=stamp),CFG,NOW)
            self.assertEqual(r["eastern_day"],day);dates.append(sync.payload(r,CFG)["date"])
        self.assertNotEqual(dates[1],dates[2])
        march=sync.Config(4,3,0,"2026-01-01T00:00:00Z")
        for stamp in ["2026-03-08T06:59:59.000Z","2026-03-08T07:00:00.000Z"]:
            self.assertEqual(sync.normalize(row(created_at=stamp),march,NOW)["eastern_day"],"2026-03-08")

    def test_private_fields_are_not_stored_or_sent(self):
        r=sync.normalize(row(email="private@example.test",token="secret-token",url="https://test.invalid/secret-token",transaction_id="affiliate-source"),CFG,NOW)
        text=sync.dump(r)+sync.dump(sync.payload(r,CFG))
        for private in ["private@example.test","secret-token","affiliate-source"]:
            self.assertNotIn(private,text)

    def test_stable_order_id_distinct_for_distinct_events(self):
        a=self.body();b=sync.payload(sync.normalize(row(102),CFG,NOW),CFG)
        self.assertEqual(a,self.body());self.assertNotEqual(a["order_id"],b["order_id"])

    def test_ingest_and_cursor_persist_across_process_restart(self):
        self.ingest();self.db.db.close();self.db=SQLiteHarness(self.path);self.store=Store(self.db,CFG)
        self.assertEqual(self.store.cursor(),101);self.assertEqual(self.store.summary(),{"ready":1})

    def test_missing_state_does_not_start_at_zero(self):
        other=Store(self.db,sync.Config(4,3,0,CFG.not_before,"another-stream"))
        with self.assertRaisesRegex(RuntimeError,"not_initialized"):
            other.cursor()

    def test_failed_ingest_rolls_back_both_rows_and_cursor(self):
        execute=self.db.execute
        def failure(sql,params=()):
            if sql.startswith("UPDATE allstate_ef_stream"):
                raise RuntimeError("injected commit-boundary failure")
            return execute(sql,params)
        with patch.object(self.db,"execute",side_effect=failure),self.assertRaises(RuntimeError):
            self.ingest()
        self.assertEqual(self.store.cursor(),100);self.assertEqual(self.store.summary(),{})

    def test_held_row_is_durable_before_ingest_checkpoint(self):
        self.ingest(row(state=""));self.assertEqual(self.store.cursor(),101)
        self.assertEqual(self.store.summary(),{"held":1})

    def test_duplicate_event_not_sent_twice(self):
        self.ingest(row(),row(102,event_id="synthetic-event-101"))
        self.assertEqual(self.store.summary(),{"held":1,"ready":1})

    def test_invalid_page_order_does_not_advance(self):
        for p in [page(row(102),row(101)),{**page(row()),"last_billing_id":"200"},page(row(100))]:
            with self.assertRaises(RuntimeError):self.store.ingest(100,p)
        self.assertEqual(self.store.cursor(),100)

    def test_empty_page_cursor_unchanged(self):
        self.store.ingest(100,page());self.assertEqual(self.store.cursor(),100)

    def test_nonadvancing_page_with_more_is_rejected(self):
        with self.assertRaisesRegex(RuntimeError,"nonadvancing"):
            self.store.ingest(100,{**page(),"has_more":True})

    def test_changed_target_configuration_refused(self):
        changed=Store(self.db,sync.Config(5,3,0,CFG.not_before))
        with self.assertRaisesRegex(RuntimeError,"config_conflict"):changed.cursor()

    def test_claim_only_once(self):
        self.ingest();self.assertTrue(self.store.claim(101));self.assertFalse(self.store.claim(101))

    def test_late_http_ack_cannot_downgrade_confirmed_readback(self):
        self.ingest();self.store.claim(101);self.store.mark(101,"confirmed",conversion_id="synthetic-conversion")
        self.store.mark(101,"received_unverified");self.store.mark(101,"ambiguous")
        self.assertEqual(self.store.summary(),{"confirmed":1})

    def test_invalid_delivery_transition_refused(self):
        self.ingest()
        with self.assertRaises(ValueError):self.store.mark(101,"ready")

    def test_ack_and_exact_readback_confirm_one_event(self):
        self.ingest();p=FakeProvider();sync.publish_ready(self.store,p);sync.publish_ready(self.store,p)
        self.assertEqual(len(p.creates),1);self.assertEqual(self.store.summary(),{"confirmed":1})

    def test_http_ack_without_readback_is_not_confirmed_or_reposted(self):
        self.ingest();p=FakeProvider(delay=True);sync.publish_ready(self.store,p);sync.publish_ready(self.store,p)
        self.assertEqual(len(p.creates),1);self.assertEqual(self.store.summary(),{"received_unverified":1})

    def test_timeout_held_ambiguous_and_never_retried(self):
        self.ingest();p=FakeProvider(fail=True);sync.publish_ready(self.store,p);sync.publish_ready(self.store,p)
        self.assertEqual(len(p.creates),1);self.assertEqual(self.store.summary(),{"ambiguous":1})
        self.assertEqual(self.store.cursor(),101)  # Ingest safe: row remains durable.

    def test_crash_after_claim_reconciles_never_reposts(self):
        self.ingest();self.store.claim(101);p=FakeProvider();sync.publish_ready(self.store,p)
        self.assertEqual(p.creates,[]);self.assertEqual(self.store.summary(),{"sending":1})
        p.records=[receipt(self.body())];sync.reconcile_one(self.store,p,101,self.body())
        self.assertEqual(self.store.summary(),{"confirmed":1})

    def test_readback_failure_before_claim_is_safe_to_retry(self):
        self.ingest();p=FakeProvider()
        with patch.object(p,"lookup",side_effect=TimeoutError()),self.assertRaises(TimeoutError):sync.publish_ready(self.store,p)
        self.assertEqual(self.store.summary(),{"ready":1});self.assertEqual(p.creates,[])
        sync.publish_ready(self.store,p);self.assertEqual(len(p.creates),1)

    def test_existing_exact_record_not_recreated(self):
        self.ingest();p=FakeProvider();p.records=[receipt(self.body())]
        sync.publish_ready(self.store,p);self.assertEqual(p.creates,[]);self.assertEqual(self.store.summary(),{"confirmed":1})

    def test_wrong_cost_or_id_or_duplicate_readback_is_conflict(self):
        body=self.body()
        for kw in [{"revenue":9},{"adv1":"wrong"},{"conversion_unix_timestamp":0},{"status":"pending"},{"currency_id":"CAD"},{"payout":1}]:
            self.assertIsNone(sync.confirmed_match(body,[receipt(body,**kw)]))
        self.assertIsNone(sync.confirmed_match(body,[receipt(body),receipt(body)]))
        self.ingest();p=FakeProvider();p.records=[receipt(body,revenue=9)];sync.publish_ready(self.store,p)
        self.assertEqual(p.creates,[]);self.assertEqual(self.store.summary(),{"conflict":1})

    def test_source_requires_known_admin_origin(self):
        with self.assertRaises(ValueError):sync.Source("https://test.invalid/customer-token","u","p")

    def test_publish_and_run_refuse_disabled_flag_before_any_connection(self):
        environment={"EF_OFFER_ID":"4","EF_REPORTING_AFFILIATE_ID":"3","EF_EVENT_ID":"0",
                     "MIRROR_NOT_BEFORE":CFG.not_before,"EF_PUBLISH":"0"}
        with patch.dict(os.environ,environment,clear=True),patch("psycopg.connect") as connect:
            for command in ["publish","run"]:
                with self.assertRaisesRegex(RuntimeError,"publication_not_enabled"):
                    sync.main([command])
            connect.assert_not_called()

    def test_provider_lookup_pagination_is_complete(self):
        ef=sync.Everflow("synthetic-key");body=self.body()
        fake=[{"conversions":[{"order_id":"other"}]*100,"paging":{"page":1,"page_size":100,"total_count":101}},
              {"conversions":[receipt(body)],"paging":{"page":2,"page_size":100,"total_count":101}}]
        with patch.object(sync,"json_request",side_effect=fake) as call:
            self.assertEqual(ef.lookup(body),[receipt(body)])
            self.assertIn("page=2",call.call_args.args[0])

    def test_incomplete_provider_pagination_is_not_absence(self):
        ef=sync.Everflow("synthetic-key")
        with patch.object(sync,"json_request",return_value={"conversions":[]}),self.assertRaises(RuntimeError):ef.lookup(self.body())


if __name__=="__main__":
    unittest.main()
