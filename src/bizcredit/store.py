
"""Real sqlite3 store: content-hash primary key, INSERT-OR-IGNORE dedup, watermark.

The ``submissions`` table tracks the *portal-side* verification state of every
material that was prepared locally.  A row in ``records`` only means the
material was ingested locally; it never implies the portal accepted it.
Submission rows are keyed by the same content hash and transition through the
states defined as ``SUBMIT_*`` constants:

* ``pending``  - registered locally, never dispatched (or dispatch not started)
* ``unknown``  - dispatched, but the portal outcome could not be confirmed
                 (timeout / connection loss / ambiguous server response)
* ``accepted`` - the portal confirmed creation / already held the record
* ``rejected`` - the portal gave a definitive refusal
"""
import json, sqlite3
from .config import DB_PATH

# Submission lifecycle states.
SUBMIT_PENDING = "pending"
SUBMIT_UNKNOWN = "unknown"
SUBMIT_ACCEPTED = "accepted"
SUBMIT_REJECTED = "rejected"
# States that still allow processing on a later run.
SUBMIT_OPEN_STATES = (SUBMIT_PENDING, SUBMIT_UNKNOWN)
# States carrying a definitive, terminal portal conclusion.
SUBMIT_TERMINAL_STATES = (SUBMIT_ACCEPTED, SUBMIT_REJECTED)


class Store:
    def __init__(self, con=None):
        self.con = con
        if con is not None:
            self._init()

    def connect(self):
        con = sqlite3.connect(DB_PATH)
        self.__init__(con)
        return con

    def _init(self):
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS records("
            "content_hash TEXT PRIMARY KEY, id TEXT, ts TEXT, payload TEXT NOT NULL)")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS meta(k TEXT PRIMARY KEY, v TEXT)")
        self.con.execute(
            "CREATE TABLE IF NOT EXISTS submissions("
            "content_hash TEXT PRIMARY KEY, id TEXT, state TEXT NOT NULL, "
            "attempts INTEGER NOT NULL DEFAULT 0, detail TEXT, updated_at TEXT)")
        self.con.commit()

    @staticmethod
    def hash(record):
        blob = json.dumps(record, sort_keys=True, ensure_ascii=False)
        import hashlib
        return hashlib.sha256(blob.encode()).hexdigest()[:24]

    def upsert(self, record):
        cur = self.con.execute(
            "INSERT OR IGNORE INTO records(content_hash,id,ts,payload) VALUES(?,?,?,?)",
            (self.hash(record), str(record.get("id", "")),
             str(record.get("ts", "")), json.dumps(record, ensure_ascii=False)))
        self.con.commit()
        return cur.rowcount > 0

    def count(self):
        return self.con.execute("SELECT COUNT(*) FROM records").fetchone()[0]

    def all_records(self):
        return [json.loads(p) for (_h, p) in
                self.con.execute("SELECT content_hash,payload FROM records")]

    def get(self, content_hash):
        row = self.con.execute(
            "SELECT payload FROM records WHERE content_hash=?", (content_hash,)).fetchone()
        return json.loads(row[0]) if row else None

    def watermark(self):
        row = self.con.execute("SELECT v FROM meta WHERE k='watermark'").fetchone()
        return row[0] if row else None

    def set_watermark(self, value):
        self.con.execute(
            "INSERT INTO meta(k,v) VALUES('watermark',?) "
            "ON CONFLICT(k) DO UPDATE SET v=excluded.v", (str(value),))
        self.con.commit()

    # ------------------------------------------------------------------ #
    # Submission / portal verification state.
    # ------------------------------------------------------------------ #
    def register_submission(self, record, state=SUBMIT_PENDING):
        """Ensure a submission row exists for ``record`` without overwriting state.

        Returns the current state, so a caller can tell a freshly registered
        material apart from one whose portal outcome is already known.
        """
        chash = self.hash(record)
        self.con.execute(
            "INSERT OR IGNORE INTO submissions(content_hash,id,state,attempts,detail,updated_at)"
            " VALUES(?,?,?,0,NULL,NULL)",
            (chash, str(record.get("id", "")), state))
        self.con.commit()
        return self.get_submission_state(chash)

    def get_submission_state(self, content_hash):
        row = self.con.execute(
            "SELECT state FROM submissions WHERE content_hash=?",
            (content_hash,)).fetchone()
        return row[0] if row else None

    def get_submission(self, content_hash):
        row = self.con.execute(
            "SELECT content_hash,id,state,attempts,detail,updated_at "
            "FROM submissions WHERE content_hash=?", (content_hash,)).fetchone()
        if not row:
            return None
        return {"content_hash": row[0], "id": row[1], "state": row[2],
                "attempts": row[3], "detail": json.loads(row[4]) if row[4] else None,
                "updated_at": row[5]}

    def all_submissions(self):
        return [self.get_submission(r[0]) for r in
                self.con.execute("SELECT content_hash FROM submissions ORDER BY rowid")]

    def update_submission(self, content_hash, state, detail=None, increment=True):
        cur = self.con.execute(
            "SELECT attempts FROM submissions WHERE content_hash=?", (content_hash,))
        row = cur.fetchone()
        attempts = (row[0] if row else 0) + (1 if increment else 0)
        from datetime import datetime, timezone
        now = datetime.now(timezone.utc).isoformat()
        self.con.execute(
            "INSERT INTO submissions(content_hash,id,state,attempts,detail,updated_at)"
            " VALUES(?,?,?,?,?,?) "
            "ON CONFLICT(content_hash) DO UPDATE SET state=excluded.state,"
            " attempts=excluded.attempts, detail=excluded.detail, updated_at=excluded.updated_at",
            (content_hash, "", state, attempts,
             json.dumps(detail, ensure_ascii=False) if detail is not None else None, now))
        self.con.commit()

    def submission_counts(self):
        out = {SUBMIT_PENDING: 0, SUBMIT_UNKNOWN: 0,
               SUBMIT_ACCEPTED: 0, SUBMIT_REJECTED: 0}
        for (state, n) in self.con.execute(
                "SELECT state, COUNT(*) FROM submissions GROUP BY state"):
            out[state] = n
        return out

    def reset(self):
        self.con.execute("DELETE FROM records")
        self.con.execute("DELETE FROM meta")
        self.con.execute("DELETE FROM submissions")
        self.con.commit()
