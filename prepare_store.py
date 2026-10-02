#!/usr/bin/env python3
"""Explicit release operation: apply mirror schema and initialize one new stream.

Never runs on service startup. No source/Everflow calls, replay, or cursor reset.
"""
import argparse
import os
import sys
from pathlib import Path

import psycopg
from store import Store
from sync import Config, database_options, dump


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--after-billing-id",type=int,required=True)
    args=parser.parse_args()
    if args.after_billing_id<0:
        raise ValueError("explicit_nonnegative_activation_cursor_required")
    cfg=Config(int(os.environ["EF_OFFER_ID"]),int(os.environ["EF_REPORTING_AFFILIATE_ID"]),int(os.environ["EF_EVENT_ID"]),os.environ["MIRROR_NOT_BEFORE"])
    dsn=os.environ["MIRROR_DATABASE_URL"]
    with psycopg.connect(dsn,**database_options(dsn)) as db:
        db.execute("SET statement_timeout='10s'")
        with db.transaction():
            db.execute(Path(__file__).with_name("schema.sql").read_text())
            Store(db,cfg).initialize(args.after_billing_id)
    print(dump({"ok":True,"initialized_cursor":args.after_billing_id,"publication":False}))


if __name__=="__main__":
    try:main()
    except Exception as exc:
        print(dump({"ok":False,"error_type":type(exc).__name__}),file=sys.stderr)
        raise SystemExit(1)
