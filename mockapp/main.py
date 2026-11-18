import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
import hashlib
import json
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from bizcredit.config import SPEC
from bizcredit.mockdata import build_rows, extra_row

app = FastAPI(title="Local source")
_ALL = build_rows()
_extra = []
_verified_sessions = set()
_accepted_tokens = set()
# Per-token injected portal behaviour for offline failure/recovery exercises:
#   "reject"         -> definitive 422 refusal, no record created
#   "timeout_before" -> 504, request never produced a record
#   "timeout_after"  -> record created, then 504 (confirmation lost)
#   "drop_after"     -> record created, then the connection drops
_submit_behavior = {}

def _data():
    return _ALL + _extra

def _page_items(page):
    lo, hi = (page - 1) * SPEC.page_size, page * SPEC.page_size
    return _data()[lo:hi]

@app.get("/items")
def items(page: int = 1):
    data = _page_items(page)
    if SPEC.fmt == "json":
        return {"items": data, "page": page}
    if SPEC.fmt == "text":
        lines = ["\t".join(str(it.get(c, "")) for c in SPEC.columns) for it in data]
        return Response(content="\n".join(lines), media_type="text/plain")
    th = "".join("<th>%s</th>" % c for c in SPEC.columns)
    trs = "".join("<tr>" + "".join("<td>%s</td>" % it.get(c, "") for c in SPEC.columns)
                  + "</tr>" for it in data)
    return Response(content="<html><table><tr>%s</tr>%s</table></html>" % (th, trs),
                    media_type="text/html")

@app.get("/refresh")
def refresh():
    _extra.append(extra_row(len(_extra) + 1))
    return {"total": len(_data())}

@app.get("/probe")
def probe():
    app.__dict__["_flaky_n"] = app.__dict__.get("_flaky_n", 0) + 1
    if app.__dict__["_flaky_n"] <= 2:
        return Response(status_code=429)
    return {"ok": True, "attempts": app.__dict__["_flaky_n"]}

@app.get("/health")
def health():
    return {"ok": True, "rows": len(_data())}

@app.post("/login")
def login(response: Response):
    sid = "SID-local"
    csrf = "CSRF-local"
    challenge = hashlib.sha256((sid + csrf).encode()).hexdigest()[:4]
    response.set_cookie("sid", sid)
    response.set_cookie("csrf", csrf)
    response.set_cookie("challenge", challenge)
    return {"role": "operator"}

@app.get("/query")
def query(request: Request, page: int = 1):
    if not request.cookies.get("sid"):
        return Response(status_code=401)
    if request.headers.get("x-csrf") != request.cookies.get("csrf"):
        return Response(status_code=403)
    items_page = _page_items(page)
    nxt = _page_items(page + 1)
    sid = request.cookies.get("sid")
    gate = (SPEC.engine == "rpa" and page == 1 and sid not in _verified_sessions
            and len(_data()) > SPEC.page_size)
    return {"items": items_page, "page": page, "has_more": bool(nxt),
            "captcha_required": bool(gate)}

@app.post("/captcha")
async def captcha(request: Request):
    body = await request.json()
    ok = body.get("code") == request.cookies.get("challenge")
    if ok:
        _verified_sessions.add(request.cookies.get("sid"))
    return {"ok": bool(ok)}

@app.post("/submit")
async def submit(request: Request):
    body = await request.json()
    token = body.get("token")
    behavior = _submit_behavior.pop(token, None)  # one-shot; later calls run normally
    if behavior == "reject":
        return JSONResponse(
            status_code=422,
            content={"created": False, "token": token,
                     "reason": "rejected by portal rule"})
    if behavior == "timeout_before":
        return Response(status_code=504)
    if behavior in ("timeout_after", "drop_after"):
        _accepted_tokens.add(token)
        if behavior == "drop_after":
            # Simulate the connection vanishing before the client gets a response.
            raise RuntimeError("simulated connection drop after record creation")
        return Response(status_code=504)
    created = token not in _accepted_tokens
    if created:
        _accepted_tokens.add(token)
    return {"created": bool(created), "token": token}


@app.get("/submissions/{token}")
def submission_lookup(token: str):
    """Idempotency reconciliation: report whether the portal holds the record."""
    if token in _accepted_tokens:
        return {"token": token, "state": "accepted"}
    return JSONResponse(status_code=404,
                        content={"token": token, "state": "missing"})


@app.post("/test/reset")
def test_reset():
    """Clear portal-side state and injected failure modes (test hook)."""
    _extra.clear()
    _verified_sessions.clear()
    _accepted_tokens.clear()
    _submit_behavior.clear()
    app.__dict__["_flaky_n"] = 0
    return {"ok": True}


@app.post("/test/fail_submit")
async def test_fail_submit(request: Request):
    """Inject a one-shot failure mode for a token (test hook)."""
    body = await request.json()
    _submit_behavior[body["token"]] = body["behavior"]
    return {"ok": True, "token": body["token"], "behavior": body["behavior"]}
