import json

import pytest

import mockapp.main as mock
from bizcredit.framework import Rpa, content_hash, run_rpa, _RUN_LOCK
from bizcredit.store import (Store, SUBMIT_PENDING, SUBMIT_UNKNOWN,
                             SUBMIT_ACCEPTED, SUBMIT_REJECTED,
                             SUBMIT_OPEN_STATES)


@pytest.fixture
def ctx():
    con = Store().connect()
    Store(con).reset()
    mock._extra.clear()
    mock._verified_sessions.clear()
    mock._accepted_tokens.clear()
    mock._submit_behavior.clear()
    mock.app.__dict__["_flaky_n"] = 0
    # The process-wide run lock must be free at the start of every test.
    if _RUN_LOCK.locked():
        _RUN_LOCK.release()
    yield con
    if _RUN_LOCK.locked():
        _RUN_LOCK.release()
    Store(con).reset()
    con.close()


def _portal_tokens():
    return set(mock._accepted_tokens)


def _unique_records(con):
    store = Store(con)
    return [(store.hash(r), r) for r in store.all_records()]


# --------------------------------------------------------------------------- #
# Happy path, idempotency and the local-save vs portal-accepted distinction.
# --------------------------------------------------------------------------- #
def test_first_run_accepts_every_material_once(ctx):
    result = run_rpa(con=ctx)
    n_unique = result["count"]
    assert n_unique == 9
    assert result["accepted"] == n_unique
    assert result["rejected"] == result["unknown"] == result["pending"] == 0
    assert result["complete"] is True
    assert result["unconfirmed"] == []
    # Exactly one portal record per unique material.
    assert len(_portal_tokens()) == n_unique
    # Historical keys / statistics are still populated.
    assert result["submit_created"] == n_unique
    assert result["submit_dedup"] == 0
    assert result["engine"] == "rpa" and result["logged_in"] is True


def test_locally_saved_record_is_not_implicitly_accepted(ctx):
    store = Store(con := ctx)
    record = {"id": "BIZ-0001", "name": "Alpha Widget", "value": 1,
              "status": "new", "ts": "2030-01-02T00:00:00Z", "holder": "h_1"}
    store.upsert(record)
    state = store.register_submission(record)
    assert state == SUBMIT_PENDING
    assert len(_portal_tokens()) == 0
    summary = store.submission_counts()
    assert summary[SUBMIT_ACCEPTED] == 0 and summary[SUBMIT_PENDING] == 1


def test_rerun_does_not_create_a_second_portal_record(ctx):
    first = run_rpa(con=ctx)
    tokens_after_first = _portal_tokens()
    second = run_rpa(con=ctx)
    # The portal never saw a second POST create the same token.
    assert _portal_tokens() == tokens_after_first
    assert second["accepted"] == first["accepted"]
    assert second["unknown"] == second["pending"] == 0
    assert second["complete"] is True

    # Local DB rebuilt while the portal still holds the records: resubmissions
    # must come back as dedup, never as a second created record.
    Store(ctx).reset()
    assert Store(ctx).count() == 0
    rebuilt = run_rpa(con=ctx)
    assert _portal_tokens() == tokens_after_first  # portal set unchanged
    assert rebuilt["submit_dedup"] == first["accepted"]
    assert rebuilt["submit_created"] == 0
    assert rebuilt["accepted"] == first["accepted"]


# --------------------------------------------------------------------------- #
# Definitive rejection.
# --------------------------------------------------------------------------- #
def test_rejection_is_distinguishable_and_not_retried(ctx):
    # Prime the local batch, then make the portal reject one token.
    rpa = Rpa()
    rpa.login()
    items = rpa.collect()
    from bizcredit.framework import normalize, dedup, _apply_kernel
    norm = _apply_kernel(dedup([normalize(dict(x)) for x in items])[0])
    target = norm[0]
    token = content_hash(target)
    mock._submit_behavior[token] = "reject"

    result = run_rpa(con=ctx)
    assert result["rejected"] == 1
    assert result["accepted"] == len(norm) - 1
    assert result["unknown"] == result["pending"] == 0
    row = Store(ctx).get_submission(token)
    assert row["state"] == SUBMIT_REJECTED
    assert "rejected by portal rule" in json.dumps(row["detail"])

    # Rerun: the rejected material is terminal and must not be re-dispatched.
    post_calls = {"n": 0}
    from bizcredit.framework import Transport
    original_transport_request = Transport.request

    def counting_request(self, method, path, **kw):
        if method == "POST" and path == "/submit":
            post_calls["n"] += 1
        return original_transport_request(self, method, path, **kw)

    Transport.request = counting_request
    try:
        again = run_rpa(con=ctx)
    finally:
        Transport.request = original_transport_request
    assert again["rejected"] == 1
    assert post_calls["n"] == 0  # terminal conclusion never resent


# --------------------------------------------------------------------------- #
# Uncertain outcome (timeout / lost confirmation) and recovery.
# --------------------------------------------------------------------------- #
def _force_ambiguous_submit(rpa):
    """Make every POST /submit fail with 504; GET lookups still work."""
    original = rpa.t.request

    def flaky(method, path, **kw):
        if method == "POST" and path == "/submit":
            return 504, {}, ""
        return original(method, path, **kw)

    rpa.t.request = flaky


def test_timeout_without_portal_record_ends_unknown_then_recovers(ctx):
    con = ctx
    # First run while the portal times out every submission.
    original_init = Rpa.__init__

    def init_with_flaky(self):
        original_init(self)
        _force_ambiguous_submit(self)

    Rpa.__init__ = init_with_flaky
    try:
        result = run_rpa(con=con)
    finally:
        Rpa.__init__ = original_init

    assert result["unknown"] == result["count"]
    assert result["complete"] is False
    assert len(result["unconfirmed"]) == result["count"]
    assert {u["state"] for u in result["unconfirmed"]} == {SUBMIT_UNKNOWN}
    # Nothing was ever created portal-side.
    assert _portal_tokens() == set()

    # Rerun against a healthy portal: reconcile (404) then dispatch exactly once.
    recovered = run_rpa(con=con)
    assert recovered["accepted"] == result["count"]
    assert recovered["unknown"] == 0
    assert len(_portal_tokens()) == result["count"]  # one record each, no dupes


def test_confirmation_lost_after_create_is_reconciled_without_resend(ctx):
    con = ctx
    # Run once so the portal holds every token.
    first = run_rpa(con=con)
    tokens = _portal_tokens()
    # Simulate a crash that wiped local conclusions but kept portal records:
    # mark every row back to "unknown", as an interrupted dispatch would.
    store = Store(con)
    for chash, _r in _unique_records(con):
        store.update_submission(chash, SUBMIT_UNKNOWN,
                                {"reason": "simulated crash"}, increment=False)
    assert store.submission_counts()[SUBMIT_UNKNOWN] == first["count"]

    recovered = run_rpa(con=con)
    assert recovered["accepted"] == first["count"]
    assert recovered["unknown"] == 0
    # Reconciliation only: the portal token set is identical (no resend).
    assert _portal_tokens() == tokens


# --------------------------------------------------------------------------- #
# Partial success and process interruption.
# --------------------------------------------------------------------------- #
def test_interrupted_run_persists_partial_state_and_resumes(ctx):
    con = ctx
    store = Store(con)
    calls = {"n": 0}
    original_update = Store.update_submission

    def crash_after_first(self, chash, state, detail=None, increment=True):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("client exited before confirmation")
        return original_update(self, chash, state, detail, increment)

    Store.update_submission = crash_after_first
    try:
        with pytest.raises(RuntimeError):
            run_rpa(con=con)
    finally:
        Store.update_submission = original_update

    counts = store.submission_counts()
    assert counts[SUBMIT_ACCEPTED] == 1
    # The in-flight material stayed open; untouched materials remain pending.
    assert counts[SUBMIT_UNKNOWN] + counts[SUBMIT_PENDING] == store.count() - 1
    partial_tokens = _portal_tokens()

    # Restart: only the open materials are processed again.
    resumed = run_rpa(con=con)
    assert resumed["accepted"] == store.count()
    assert resumed["complete"] is True
    # Portal-side records never duplicated despite the mid-dispatch crash.
    assert len(_portal_tokens()) == store.count()
    assert partial_tokens <= _portal_tokens()


def test_unconfirmed_list_identifies_exactly_what_is_left(ctx):
    con = ctx
    rpa_holder = {}
    original_init = Rpa.__init__

    def init_flaky(self):
        original_init(self)
        _force_ambiguous_submit(self)
        rpa_holder["rpa"] = self

    Rpa.__init__ = init_flaky
    try:
        result = run_rpa(con=con)
    finally:
        Rpa.__init__ = original_init

    open_ids = {u["id"] for u in result["unconfirmed"]}
    all_ids = {r.get("id", "") for r in Store(con).all_records()}
    assert open_ids == all_ids  # every material still unconfirmed is listed
    # Read-only status view agrees with the run result.
    from bizcredit.framework import submission_status
    status = submission_status(con=con)
    assert status["complete"] is False
    assert {u["content_hash"] for u in status["unconfirmed"]} == \
           {u["content_hash"] for u in result["unconfirmed"]}


# --------------------------------------------------------------------------- #
# Duplicate clicks / concurrent runs inside one process.
# --------------------------------------------------------------------------- #
def test_concurrent_run_is_skipped_instead_of_duplicating(ctx):
    _RUN_LOCK.acquire()  # stand in for a run already in progress
    try:
        result = run_rpa(con=ctx)
    finally:
        _RUN_LOCK.release()
    assert result.get("skipped") is True
    assert result["count"] == Store(ctx).count()
    assert _portal_tokens() == set()  # nothing dispatched by the skipped run


# --------------------------------------------------------------------------- #
# Hard connection drop: the portal may already hold the record.
# --------------------------------------------------------------------------- #
def test_connection_drop_after_create_is_reconciled_not_duplicated(ctx):
    con = ctx
    # Make the first POST of the run raise (connection gone) after the portal
    # created the record; all later calls behave normally.
    from bizcredit.framework import Transport
    original_request = Transport.request
    drops = {"left": 1}

    def dropping_request(self, method, path, **kw):
        if method == "POST" and path == "/submit" and drops["left"]:
            drops["left"] -= 1
            # Mirror the mock portal: record exists server-side, then the
            # connection disappears before the client receives anything.
            body = kw.get("json_body") or {}
            mock._accepted_tokens.add(body["token"])
            raise RuntimeError("simulated connection drop")
        return original_request(self, method, path, **kw)

    Transport.request = dropping_request
    try:
        result = run_rpa(con=con)
    finally:
        Transport.request = original_request

    assert result["accepted"] == result["count"]
    assert result["unknown"] == 0
    # The dropped token was reconciled (one portal record each, no duplicate).
    assert len(_portal_tokens()) == result["count"]
    # Every material's detail trail records how its conclusion was reached.
    states = {s["state"] for s in result["submissions"]}
    assert states == {SUBMIT_ACCEPTED}


# --------------------------------------------------------------------------- #
# Captcha recovery.
# --------------------------------------------------------------------------- #
def test_captcha_failure_is_retried_and_run_completes(ctx):
    rpa_holder = {}
    original_init = Rpa.__init__

    def init_with_captcha_flake(self):
        original_init(self)
        rpa_holder["rpa"] = self
        original_request = self.t.request
        captcha_calls = {"n": 0}

        def flaky(method, path, **kw):
            if method == "POST" and path == "/captcha":
                captcha_calls["n"] += 1
                if captcha_calls["n"] == 1:
                    return 200, {}, json.dumps({"ok": False})
            return original_request(method, path, **kw)

        self.t.request = flaky

    Rpa.__init__ = init_with_captcha_flake
    try:
        result = run_rpa(con=ctx)
    finally:
        Rpa.__init__ = original_init

    assert result["captcha_solved"] is True
    rpa = rpa_holder["rpa"]
    assert "captcha:failed" in rpa.log and "captcha:solved" in rpa.log
    assert rpa.log.index("captcha:failed") < rpa.log.index("captcha:solved")
    assert result["accepted"] == result["count"]


def test_pending_local_row_is_dispatched_on_next_run(ctx):
    con = ctx
    store = Store(con)
    # A material saved locally (registered pending) but never dispatched.
    record = {"id": "BIZ-9001", "name": "Solo", "value": 7, "status": "new",
              "ts": "2031-01-01T00:00:00Z", "holder": "h_9"}
    store.upsert(record)
    assert store.register_submission(record) == SUBMIT_PENDING
    assert store.submission_counts()[SUBMIT_ACCEPTED] == 0

    # A full run collects the source batch and also clears the pending row.
    result = run_rpa(con=con)
    chash = Store.hash(record)
    assert store.get_submission_state(chash) == SUBMIT_ACCEPTED
    assert result["complete"] is True


def test_stuck_unknown_stays_unconfirmed_and_recovers_on_later_run(ctx):
    con = ctx
    original_init = Rpa.__init__

    def init_flaky(self):
        original_init(self)
        _force_ambiguous_submit(self)

    Rpa.__init__ = init_flaky
    try:
        run_rpa(con=con)                       # everything ends unknown
        second = run_rpa(con=con)              # still a failing portal
    finally:
        Rpa.__init__ = original_init
    assert second["unknown"] == second["count"]
    assert second["complete"] is False
    assert _portal_tokens() == set()

    third = run_rpa(con=con)                   # portal healthy now
    assert third["accepted"] == third["count"]
    assert third["unknown"] == 0 and third["complete"] is True
    assert len(_portal_tokens()) == third["count"]
