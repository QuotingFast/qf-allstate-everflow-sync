"""Durable PostgreSQL state. Schema application is an explicit release step."""
from sync import Held, billing_id, digest, dump, normalize, payload


class Store:
    def __init__(self, connection, cfg):
        self.db,self.cfg=connection,cfg

    def cursor(self):
        row=self.db.execute("SELECT cursor_id,config_hash FROM allstate_ef_stream WHERE stream=%s",(self.cfg.stream,)).fetchone()
        if row is None:
            raise RuntimeError("stream_not_initialized_no_replay")
        if row[1]!=digest(self.cfg.__dict__):
            raise RuntimeError("approved_config_conflict")
        return row[0]

    def initialize(self, after):
        with self.db.transaction():
            self.db.execute("INSERT INTO allstate_ef_stream(stream,cursor_id,not_before,config_hash) VALUES(%s,%s,%s,%s)",
                            (self.cfg.stream,after,self.cfg.not_before,digest(self.cfg.__dict__)))

    def ingest(self, after, page):
        rows=page["records"]
        if len(rows)>500:
            raise RuntimeError("source_page_too_large")
        ids=[billing_id(r.get("billing_id")) for r in rows]
        if ids!=sorted(set(ids)) or any(i<=after for i in ids):
            raise RuntimeError("invalid_source_cursor_order")
        last=ids[-1] if ids else after
        if str(page.get("last_billing_id"))!=str(last) or type(page.get("has_more")) is not bool:
            raise RuntimeError("invalid_source_cursor_receipt")
        if not rows and page["has_more"]:
            raise RuntimeError("nonadvancing_source_page")
        with self.db.transaction():
            current=self.db.execute("SELECT cursor_id,config_hash FROM allstate_ef_stream WHERE stream=%s FOR UPDATE",(self.cfg.stream,)).fetchone()
            if not current or current[0]!=after or current[1]!=digest(self.cfg.__dict__):
                raise RuntimeError("cursor_or_approved_config_conflict")
            for raw,bid in zip(rows,ids):
                try:
                    record=normalize(raw,self.cfg);body=payload(record,self.cfg)
                    state,reason,event_key="ready",None,record["event_id"]
                except Held as exc:
                    # Invalid source fields can contain PII: retain only ID/reason.
                    record,body,event_key={"billing_id":bid},None,None
                    state,reason="held",str(exc)
                if event_key:
                    prior=self.db.execute("SELECT billing_id FROM allstate_ef_outbox WHERE stream=%s AND event_key=%s",(self.cfg.stream,event_key)).fetchone()
                    if prior:
                        state,reason,event_key="held","duplicate_or_conflicting_source_event",None
                self.db.execute("INSERT INTO allstate_ef_outbox(stream,billing_id,event_key,source_hash,record_json,payload_json,state,reason) VALUES(%s,%s,%s,%s,%s,%s,%s,%s)",
                                (self.cfg.stream,bid,event_key,digest(record),dump(record),dump(body) if body else None,state,reason))
            # Advancing this ingest cursor is safe only because every row, including
            # holds, is durable in the same transaction. It is NOT a delivery ACK.
            self.db.execute("UPDATE allstate_ef_stream SET cursor_id=%s WHERE stream=%s",(last,self.cfg.stream))
        return len(rows)

    def candidates(self, states, limit=100):
        return self.db.execute("SELECT billing_id,payload_json,state FROM allstate_ef_outbox WHERE stream=%s AND state=ANY(%s) ORDER BY billing_id LIMIT %s",(self.cfg.stream,list(states),limit)).fetchall()

    def claim(self, bid):
        with self.db.transaction():
            row=self.db.execute("UPDATE allstate_ef_outbox SET state='sending',attempts=attempts+1,attempted_at=CURRENT_TIMESTAMP,updated_at=CURRENT_TIMESTAMP WHERE stream=%s AND billing_id=%s AND state='ready' RETURNING billing_id",(self.cfg.stream,bid)).fetchone()
        return row is not None

    def mark(self, bid, state, reason=None, conversion_id=None):
        if state in {"ambiguous","received_unverified"}:
            # A concurrent readback may already have confirmed or held this row.
            allowed="state='sending'"
        elif state in {"confirmed","conflict"}:
            allowed="state IN ('ready','sending','ambiguous','received_unverified')"
        else:
            raise ValueError("invalid_delivery_transition")
        with self.db.transaction():
            self.db.execute("UPDATE allstate_ef_outbox SET state=%s,reason=%s,conversion_id=%s,updated_at=CURRENT_TIMESTAMP WHERE stream=%s AND billing_id=%s AND "+allowed,(state,reason,conversion_id,self.cfg.stream,bid))

    def summary(self):
        return dict(self.db.execute("SELECT state,COUNT(*) FROM allstate_ef_outbox WHERE stream=%s GROUP BY state",(self.cfg.stream,)).fetchall())
