"""Local transport, parsing, storage, and session helpers."""
from __future__ import annotations
import hashlib
import importlib
import json
import threading

from lxml import html as _lh

from .config import SPEC
from .store import (Store, SUBMIT_PENDING, SUBMIT_UNKNOWN,
                    SUBMIT_ACCEPTED, SUBMIT_REJECTED, SUBMIT_OPEN_STATES)


# --------------------------------------------------------------------------- #
# Transport helpers.
# --------------------------------------------------------------------------- #
class Transport:
    """Client for the bundled local service."""

    def __init__(self):
        import os
        import sys
        for p in (str(SPEC_ROOT()), str(SPEC_ROOT() / "src")):
            if p not in sys.path:
                sys.path.insert(0, p)
        from starlette.testclient import TestClient
        app = importlib.import_module("mockapp.main").app
        self.client = TestClient(app)
        self.cookies: dict[str, str] = {}

    def request(self, method, path, params=None, json_body=None, headers=None):
        q = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        resp = self.client.request(method, path, params=q, json=json_body,
                                   headers=headers or {}, cookies=self.cookies)
        for k, v in resp.cookies.items():
            self.cookies[k] = v
        return resp.status_code, dict(resp.headers), resp.text


def SPEC_ROOT():
    from .config import ROOT
    return ROOT


class Sleeper:
    """Records requested delays without blocking the caller."""

    def __init__(self):
        self.slept = 0.0
        self.calls = 0

    def sleep(self, seconds):
        self.slept += seconds
        self.calls += 1


class Http:
    """HTTP client with bounded retry support."""

    def __init__(self, transport: Transport, sleeper: Sleeper,
                 max_retries=4, base_backoff=0.5, politeness=0.25):
        self.t = transport
        self.sleeper = sleeper
        self.max_retries = max_retries
        self.base_backoff = base_backoff
        self.politeness = politeness

    def get(self, path, params=None, headers=None):
        attempt = 0
        while True:
            self.sleeper.sleep(self.politeness)
            status, hdr, body = self.t.request("GET", path, params, headers=headers)
            if status in (429, 500, 502, 503) and attempt < self.max_retries:
                self.sleeper.sleep(self.base_backoff * (2 ** attempt))
                attempt += 1
                continue
            return status, hdr, body, attempt


# --------------------------------------------------------------------------- #
# Response parsing.
# --------------------------------------------------------------------------- #
def _coerce(name, val):
    if name in SPEC.int_fields:
        try:
            return int(val)
        except (TypeError, ValueError):
            return val
    return val


def parse(resp_body, fmt):
    cols = SPEC.columns
    if fmt == "json":
        items = json.loads(resp_body).get("items", [])
        return [{c: _coerce(c, it.get(c)) for c in cols} for it in items]
    if fmt == "html":
        doc = _lh.fromstring(resp_body)
        rows = []
        for tr in doc.xpath("//table//tr[./td]"):
            cells = [td.text_content().strip() for td in tr.xpath("./td")]
            if len(cells) != len(cols):
                continue
            rows.append({c: _coerce(c, v) for c, v in zip(cols, cells)})
        return rows
    if fmt == "text":
        out = []
        for line in resp_body.splitlines():
            if not line.strip():
                continue
            parts = [p.strip() for p in line.split("\t")]
            d = {c: _coerce(c, parts[i]) if i < len(parts) else _coerce(c, "")
                 for i, c in enumerate(cols)}
            out.append(d)
        return out
    raise ValueError("unknown fmt: %s" % fmt)


def fetch_all(http: Http):
    """Collect records from the local source."""
    records, attempts, page = [], 0, 1
    while True:
        status, _hdr, body, att = http.get("/" + SPEC.endpoint, params={"page": page})
        attempts += att + 1
        if status != 200:
            raise RuntimeError("fetch failed status=%s page=%s" % (status, page))
        page_recs = parse(body, SPEC.fmt)
        records.extend(page_recs)
        if len(page_recs) < SPEC.page_size:
            break
        page += 1
        if page > 200:
            break
    return records, attempts


# --------------------------------------------------------------------------- #
# Record identity helpers.
# --------------------------------------------------------------------------- #
def canonical(d):
    return json.dumps(d, sort_keys=True, ensure_ascii=False)


def content_hash(d):
    return hashlib.sha256(canonical(d).encode()).hexdigest()[:24]


def dedup(records):
    seen, out, dup = set(), [], 0
    for r in records:
        h = content_hash(r)
        if h in seen:
            dup += 1
            continue
        seen.add(h)
        out.append(r)
    return out, dup


def normalize(r):
    d = {k: (v.strip() if isinstance(v, str) else v) for k, v in r.items()}
    if isinstance(d.get("status"), str):
        d["status"] = d["status"].lower()
    return d


def _apply_kernel(records):
    k = importlib.import_module(".kernel", __package__)
    for r in records:
        if hasattr(k, "process"):
            r.update(k.process(r))
    if hasattr(k, "reduce"):
        records = k.reduce(records)
    return records


# --------------------------------------------------------------------------- #
# Collection pipeline.
# --------------------------------------------------------------------------- #
def run(mode="full", con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    sleeper = Sleeper()
    http = Http(Transport(), sleeper)
    try:
        records, attempts = fetch_all(http)
        norm = [normalize(dict(r)) for r in records]
        norm, dup_in_batch = dedup(norm)
        norm = _apply_kernel(norm)

        wm = store.watermark()
        if mode == "incremental" and wm:
            norm = [r for r in norm if str(r.get("ts", "")) > wm]

        ingested = sum(1 for r in norm if store.upsert(r))
        total = store.count()
        new_wm = max([str(r.get("ts", "")) for r in norm] + [wm or ""])
        if new_wm:
            store.set_watermark(new_wm)
        return {"engine": "crawl", "mode": mode, "fetched": len(records),
                "duplicates": dup_in_batch, "ingested": ingested, "count": total,
                "http_attempts": attempts, "slept": round(sleeper.slept, 4),
                "watermark": store.watermark()}
    finally:
        if own:
            con.close()


# --------------------------------------------------------------------------- #
# Session pipeline.
# --------------------------------------------------------------------------- #
# HTTP outcomes that never tell us whether the portal created a record.
_AMBIGUOUS_STATUS = (408, 429, 500, 502, 503, 504)
# Definitive refusal statuses.
_REJECT_STATUS = (400, 403, 422)
_SUBMIT_MAX_ATTEMPTS = 3


class Rpa:
    def __init__(self):
        self.t = Transport()
        self.sleeper = Sleeper()
        self.log = []
        self.captcha_solved = False

    def login(self):
        status, _h, _b = self.t.request("POST", "/login",
                                        json_body={"u": "operator", "p": "local"})
        if status != 200:
            raise RuntimeError("login failed status=%s" % status)
        self.log.append("login")
        return True

    def _csrf(self):
        return {"x-csrf": self.t.cookies.get("csrf", "")}

    def _query_page(self, page):
        status, _h, body = self.t.request("GET", "/query", params={"page": page},
                                          headers=self._csrf())
        if status != 200:
            raise RuntimeError("query failed status=%s" % status)
        return json.loads(body)

    def collect(self):
        out, page = [], 1
        while True:
            self.sleeper.sleep(0.1)
            data = self._query_page(page)
            out.extend(data.get("items", []))
            if data.get("captcha_required") and not self.captcha_solved:
                ok = self._solve_captcha()
                if not ok:
                    raise RuntimeError("captcha solve rejected")
                self.captcha_solved = True
                page += 1
                continue
            if not data.get("has_more"):
                break
            page += 1
            if page > 200:
                break
        self.log.append("collect:%d" % len(out))
        return out

    def _solve_captcha(self):
        # Bounded retries so a single rejected challenge does not kill the run;
        # the portal's challenge cookie stays valid between attempts.
        for attempt in range(3):
            status, _h, body = self.t.request(
                "POST", "/captcha",
                json_body={"code": self.t.cookies.get("challenge", "")},
                headers=self._csrf())
            ok = status == 200 and json.loads(body).get("ok") is True
            self.log.append("captcha:%s" % ("solved" if ok else "failed"))
            if ok:
                return True
            self.sleeper.sleep(0.5 * (2 ** attempt))
        return False

    def submit(self, record):
        """Backwards-compatible single-shot submit (created vs. dedup).

        New code should use :meth:`submit_outcome`, which also handles
        ambiguous transport outcomes.  Both use the same idempotency token,
        so mixing them can never create a second portal record.
        """
        outcome = self.submit_outcome(record)
        return outcome["state"] == SUBMIT_ACCEPTED and outcome["detail"].get("created", False)

    def reconcile(self, token):
        """Ask the portal whether an earlier dispatch actually landed.

        Returns ``(state, detail)`` where state is one of the SUBMIT_*
        constants.  A missing portal record is a *definitive* answer to the
        lookup ("not there yet") but is reported as ``unknown`` when the
        lookup itself cannot be completed, so the material stays retriable.
        """
        attempts = 0
        while True:
            try:
                status, _h, body = self.t.request(
                    "GET", "/submissions/" + token, headers=self._csrf())
            except Exception as exc:  # connection dropped before any answer
                if attempts >= _SUBMIT_MAX_ATTEMPTS - 1:
                    return SUBMIT_UNKNOWN, {"reason": "lookup_error", "error": str(exc)}
            else:
                if status == 200:
                    data = json.loads(body)
                    return SUBMIT_ACCEPTED, {"created": False,
                                             "reconciled": True,
                                             "token": data.get("token", token)}
                if status == 404:
                    return SUBMIT_PENDING, {"reconciled": True, "portal_held": False}
                if status not in _AMBIGUOUS_STATUS:
                    return SUBMIT_REJECTED, {"reason": "lookup_rejected", "status": status}
                if attempts >= _SUBMIT_MAX_ATTEMPTS - 1:
                    return SUBMIT_UNKNOWN, {"reason": "lookup_timeout", "status": status}
            attempts += 1
            self.sleeper.sleep(0.5 * (2 ** attempts))

    def submit_outcome(self, record):
        """Dispatch one material and return a distinguishable conclusion.

        Returns a dict ``{"state", "detail"}``.  ``state`` is always one of
        ``accepted`` / ``rejected`` / ``unknown``; ``unknown`` means the
        portal outcome could not be confirmed and the material must be
        reconciled (never blindly resent) on a later run.
        """
        token = content_hash(record)
        attempt = 0
        while True:
            try:
                status, _h, body = self.t.request(
                    "POST", "/submit",
                    json_body={"token": token, "record": record},
                    headers=self._csrf())
            except Exception as exc:
                # Connection vanished: the request may have been processed.
                if attempt >= _SUBMIT_MAX_ATTEMPTS - 1:
                    state, detail = self.reconcile(token)
                    if state == SUBMIT_PENDING:  # portal confirms no record
                        state, detail = SUBMIT_UNKNOWN, {
                            "reason": "transport_error", "error": str(exc)}
                    return {"state": state,
                            "detail": {**detail, "attempts": attempt + 1}}
            else:
                if status == 200:
                    data = json.loads(body)
                    created = bool(data.get("created"))
                    self.log.append("submit:%s" % ("created" if created else "dedup"))
                    return {"state": SUBMIT_ACCEPTED,
                            "detail": {"created": created, "attempts": attempt + 1}}
                if status in _REJECT_STATUS:
                    self.log.append("submit:rejected")
                    detail = {"status": status}
                    try:
                        detail.update(json.loads(body))
                    except (ValueError, TypeError):
                        pass
                    return {"state": SUBMIT_REJECTED,
                            "detail": {**detail, "attempts": attempt + 1}}
                if status not in _AMBIGUOUS_STATUS:
                    self.log.append("submit:rejected")
                    return {"state": SUBMIT_REJECTED,
                            "detail": {"status": status, "attempts": attempt + 1}}
                if attempt >= _SUBMIT_MAX_ATTEMPTS - 1:
                    # Ambiguous after every retry: reconcile before giving up.
                    state, detail = self.reconcile(token)
                    if state == SUBMIT_PENDING:
                        state, detail = SUBMIT_UNKNOWN, {"reason": "unconfirmed"}
                    self.log.append("submit:%s" % state)
                    return {"state": state,
                            "detail": {**detail, "attempts": attempt + 1}}
            attempt += 1
            self.sleeper.sleep(0.5 * (2 ** (attempt - 1)))


# A process-wide guard so two concurrent callers (e.g. repeated clicks while
# the first run is still working) cannot dispatch the same batch twice.
_RUN_LOCK = threading.Lock()


def _claim_pending(store, chash):
    """Atomically move one ``pending`` submission to ``unknown`` as a claim.

    Returns True when this caller won the claim.  The row is only updated if
    it is still ``pending``, which makes the claim safe against both
    in-process concurrency and a crash mid-dispatch: after a crash the
    material simply stays ``unknown`` and is reconciled on the next run.
    """
    cur = store.con.execute(
        "UPDATE submissions SET state=? WHERE content_hash=? AND state=?",
        (SUBMIT_UNKNOWN, chash, SUBMIT_PENDING))
    store.con.commit()
    return cur.rowcount > 0


def _reclaim_unknown(store, chash):
    """Conditional re-claim of an ``unknown`` row the portal says it lacks.

    The lookup already proved no portal record exists, so re-dispatching under
    the same idempotency token is safe.  The guard keeps a concurrent caller
    that may have advanced the row in the meantime from double-dispatching.
    """
    cur = store.con.execute(
        "UPDATE submissions SET state=? WHERE content_hash=? AND state=?",
        (SUBMIT_UNKNOWN, chash, SUBMIT_UNKNOWN))
    store.con.commit()
    return cur.rowcount > 0


def _summarize(store, count, r, items_len, dup, ingested):
    counts = store.submission_counts()
    submissions = store.all_submissions()
    open_rows = [s for s in submissions if s["state"] in SUBMIT_OPEN_STATES]
    result = {
        "engine": "rpa",
        "logged_in": True,
        "captcha_solved": r.captcha_solved,
        "collected": items_len,
        "fetched": items_len,
        "duplicates": dup,
        "ingested": ingested,
        "count": count,
        # Historical keys / statistics remain available.
        "submit_created": sum(
            1 for s in submissions
            if s["state"] == SUBMIT_ACCEPTED
            and (s.get("detail") or {}).get("created") is True),
        "submit_dedup": sum(
            1 for s in submissions
            if s["state"] == SUBMIT_ACCEPTED
            and (s.get("detail") or {}).get("created") is False),
        # Distinguishable portal conclusions.
        "accepted": counts[SUBMIT_ACCEPTED],
        "rejected": counts[SUBMIT_REJECTED],
        "unknown": counts[SUBMIT_UNKNOWN],
        "pending": counts[SUBMIT_PENDING],
        "submissions": submissions,
        "unconfirmed": [
            {"content_hash": s["content_hash"], "id": s["id"], "state": s["state"]}
            for s in open_rows],
        "complete": not open_rows,
        "log": r.log,
    }
    return result


def run_rpa(con=None):
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    r = Rpa()
    if not _RUN_LOCK.acquire(blocking=False):
        # Another run is already dispatching; report the current durable state
        # instead of starting a second, duplicate-producing run.
        return _summarize(store, store.count(), r, 0, 0, 0) | {
            "skipped": True, "reason": "run_in_progress", "logged_in": False}
    try:
        r.login()
        items = r.collect()
        norm = [normalize(dict(it)) for it in items]
        norm, dup = dedup(norm)
        norm = _apply_kernel(norm)
        ingested = sum(1 for x in norm if store.upsert(x))

        # Registration is durable and side-effect free: a locally saved
        # material is explicitly "pending", never implicitly "accepted".
        for x in norm:
            store.register_submission(x)

        total = store.count()
        # The durable submission table is the work queue, not just the current
        # fetch: a material registered earlier (or stranded by an earlier run)
        # must still be processed even if the portal listing changed.
        by_hash = {Store.hash(x): x for x in norm}
        open_rows = [s for s in store.all_submissions()
                     if s["state"] in SUBMIT_OPEN_STATES]
        for sub in open_rows:
            chash = sub["content_hash"]
            x = by_hash.get(chash) or store.get(chash)
            if x is None:
                continue
            prior = sub["state"]
            if prior == SUBMIT_UNKNOWN:
                # An earlier run died or timed out: reconcile before any resend.
                state, detail = r.reconcile(chash)
                if state == SUBMIT_PENDING:
                    # Portal confirms it never received the record: re-claim
                    # (it is still held locally as unknown) and dispatch now.
                    if not _reclaim_unknown(store, chash):
                        continue
                    outcome = r.submit_outcome(x)
                    store.update_submission(
                        chash, outcome["state"], outcome["detail"], increment=True)
                else:
                    store.update_submission(chash, state, detail, increment=False)
                continue
            # Fresh material: claim it (state -> unknown) before dispatching,
            # so an interruption leaves a durable, reconciliable trace.
            if not _claim_pending(store, chash):
                continue
            outcome = r.submit_outcome(x)
            store.update_submission(
                chash, outcome["state"], outcome["detail"], increment=True)

        wm = max([str(x.get("ts", "")) for x in norm] + [""])
        if wm:
            store.set_watermark(wm)
        return _summarize(store, total, r, len(items), dup, ingested)
    finally:
        _RUN_LOCK.release()
        if own:
            con.close()


def submission_status(con=None):
    """Read-only view of durable portal verification state."""
    own = con is None
    con = con or Store().connect()
    store = Store(con)
    try:
        counts = store.submission_counts()
        submissions = store.all_submissions()
        open_rows = [s for s in submissions if s["state"] in SUBMIT_OPEN_STATES]
        return {"engine": "rpa", "counts": counts,
                "submissions": submissions,
                "unconfirmed": [
                    {"content_hash": s["content_hash"], "id": s["id"], "state": s["state"]}
                    for s in open_rows],
                "complete": not open_rows}
    finally:
        if own:
            con.close()
