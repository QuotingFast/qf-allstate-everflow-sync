"""Synthetic integration tests. Requires an explicitly provided LOCAL Unix socket.

Creates/drops only a unique ef_test_* schema, never accesses production data.
"""
import os
import ssl
import threading
import unittest
import uuid
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

from store import Store
from test_sync import CFG, FakeProvider, page, row
import sync

DSN=os.environ.get("TEST_POSTGRES_DSN")


@unittest.skipUnless(DSN,"TEST_POSTGRES_DSN is unset; PostgreSQL integration skipped")
class PostgreSQLTests(unittest.TestCase):
    def setUp(self):
        import psycopg
        from psycopg.conninfo import conninfo_to_dict
        host=conninfo_to_dict(DSN).get("host","")
        if not (host in {"/tmp","/private/tmp"} or host.startswith(("/tmp/","/private/tmp/"))):
            raise RuntimeError("test_database_must_use_temporary_local_socket")
        self.psycopg=psycopg
        self.schema="ef_test_"+uuid.uuid4().hex
        self.admin=psycopg.connect(DSN,autocommit=True)
        self.admin.execute(f'CREATE SCHEMA "{self.schema}"')
        self.db=self.connect()
        self.db.execute(Path(__file__).with_name("schema.sql").read_text())
        self.store=Store(self.db,CFG);self.store.initialize(100)

    def connect(self):
        db=self.psycopg.connect(DSN,autocommit=True)
        db.execute(f'SET search_path TO "{self.schema}"')
        db.execute("SET statement_timeout='5s'")
        return db

    def tearDown(self):
        self.db.close()
        self.admin.execute(f'DROP SCHEMA "{self.schema}" CASCADE')
        self.admin.close()

    def test_real_postgres_schema_cursor_and_restart_recovery(self):
        self.store.ingest(100,page(row(),row(102,state="")))
        self.db.close();self.db=self.connect();self.store=Store(self.db,CFG)
        self.assertEqual(self.store.cursor(),102)
        self.assertEqual(self.store.summary(),{"ready":1,"held":1})
        provider=FakeProvider();sync.publish_ready(self.store,provider)
        self.assertEqual(len(provider.creates),1)
        self.assertEqual(self.store.summary(),{"confirmed":1,"held":1})

    def test_two_concurrent_workers_cannot_both_claim(self):
        self.store.ingest(100,page(row()))
        barrier=threading.Barrier(2)
        def claim():
            with self.connect() as conn:
                worker=Store(conn,CFG);barrier.wait(timeout=5)
                return worker.claim(101)
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(claim) for _ in range(2)]
            self.assertEqual(sorted(f.result() for f in futures),[False,True])
        self.assertEqual(self.db.execute("SELECT attempts FROM allstate_ef_outbox").fetchone()[0],1)

    def test_concurrent_ingest_serializes_and_preserves_exactly_one_page(self):
        barrier=threading.Barrier(2)
        def ingest():
            with self.connect() as conn:
                worker=Store(conn,CFG);barrier.wait(timeout=5)
                try:return worker.ingest(100,page(row()))
                except RuntimeError:return "cursor_conflict"
        with ThreadPoolExecutor(max_workers=2) as pool:
            futures=[pool.submit(ingest) for _ in range(2)]
            values=[f.result() for f in futures]
        self.assertCountEqual(values,[1,"cursor_conflict"])
        self.assertEqual(self.store.cursor(),101)
        self.assertEqual(self.store.summary(),{"ready":1})

    def test_database_constraint_failure_rolls_back_whole_page_and_cursor(self):
        self.db.execute("ALTER TABLE allstate_ef_outbox ADD CHECK (billing_id <> 102)")
        with self.assertRaises(self.psycopg.errors.CheckViolation):
            self.store.ingest(100,page(row(),row(102)))
        self.assertEqual(self.store.cursor(),100)
        self.assertEqual(self.store.summary(),{})

    def test_crashed_submission_remains_ambiguous_across_new_connection(self):
        self.store.ingest(100,page(row()));self.store.claim(101)
        self.db.close();self.db=self.connect();self.store=Store(self.db,CFG)
        provider=FakeProvider();sync.publish_ready(self.store,provider)
        self.assertEqual(provider.creates,[])
        self.assertEqual(self.store.summary(),{"sending":1})

    def test_production_connection_forces_verified_tls(self):
        opts=sync.database_options("postgresql://example:example@db.example.test/test?sslmode=disable")
        self.assertEqual(opts["sslmode"],"verify-full")
        self.assertEqual(opts["sslrootcert"],ssl.get_default_verify_paths().cafile or "system")
        with self.assertRaises(RuntimeError):sync.database_options("dbname=postgres")

    def test_explicit_preparation_is_atomic_and_never_resets_cursor(self):
        import prepare_store
        from psycopg.conninfo import make_conninfo
        self.db.execute("DELETE FROM allstate_ef_stream")
        isolated=make_conninfo(DSN,options=f'-c search_path={self.schema}')
        environment={"MIRROR_DATABASE_URL":isolated,"MIRROR_NOT_BEFORE":CFG.not_before,
                     "EF_OFFER_ID":"4","EF_REPORTING_AFFILIATE_ID":"3","EF_EVENT_ID":"0"}
        with patch.dict(os.environ,environment,clear=True),patch("sys.argv",["prepare_store.py","--after-billing-id","100"]),patch("builtins.print"):
            prepare_store.main()
            self.store.ingest(100,page(row()))
            with self.assertRaises(self.psycopg.errors.UniqueViolation):prepare_store.main()
        self.assertEqual(self.store.cursor(),101)
        self.assertEqual(self.store.summary(),{"ready":1})
