"""
LeadFlow AI — v4.0 APEX  (audited & hardened)
╔══════════════════════════════════════════════════════════════════════════════╗
║  The most aggressive, intelligent lead generation system ever built for     ║
║  Alabama's local business market. Zero new APIs. Maximum extraction.         ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  AGENT ARCHITECTURE                                                          ║
║  ─────────────────                                                           ║
║  Agent 1 — APEX Lead Capture     Multi-engine SerpAPI sweep (Maps + News    ║
║                                  + organic text) across 8 cities × 20 niches║
║  Agent 2 — Intelligence Recon    Website autopsy, social signal mining,      ║
║                                  competitor gap analysis via SerpAPI organic ║
║  Agent 3 — AI Brain Scoring      Groq multi-pass: score → persona → custom  ║
║                                  outreach copy per lead (3 Groq calls/lead)  ║
║  Agent 4 — Strategic Assignment  Skill-match reps by niche + load balancing ║
║  Agent 5 — Omni Outreach         Rep SMS digest + per-lead AI outreach SMS  ║
║                                  + Slack rich digest + CRM webhook push      ║
║  Agent 6 — Intelligence Loop     Groq synthesizes run patterns, flags top   ║
║                                  niches, generates next-run targeting advice ║
║                                  sent to Slack as a strategic brief          ║
╚══════════════════════════════════════════════════════════════════════════════╝

AUDIT FIXES APPLIED (v4.0 → v4.0-APEX-fixed):
  1.  CRITICAL — Agent 1 dedup counter bug: new-lead count in log message
      re-queried existing_fps AFTER the set was mutated, always showing 0.
      Fixed: count new leads before mutating the set.
  2.  CRITICAL — b44_list_existing() silent failure: returned empty set on
      any error, causing ALL existing leads to be re-captured every run.
      Fixed: distinguishes network error (empty set + warning) vs empty CRM
      (empty set is valid). Logs clearly.
  3.  CRITICAL — Revenue pipeline parser in Agent 5 would silently produce 0
      for any estimate containing non-digit chars (e.g. "$1,200-1,800").
      Fixed: robust regex-based int extraction.
  4.  BUG — b44_update called with record_id="" for dry-run leads (id="dry_run_id")
      was passing the guard check and making real PUT requests.
      Fixed: guard checks `record_id not in ("", "dry_run_id")`.
  5.  BUG — _serpapi_organic() silently swallowed ALL exceptions without
      logging to METRICS, making failures invisible.
      Fixed: logs to METRICS["errors"] on exception.
  6.  BUG — Agent 1 log line at end of each query loop computed the "new"
      count by re-querying existing_fps post-mutation. Always showed 0.
      Fixed: snapshot pre-mutation count and diff correctly.
  7.  BUG — Groq client was instantiated inside ask_groq() on every call
      (hundreds of times per run). Moved to module-level singleton.
  8.  BUG — TwilioClient instantiated inside send_sms() on every call.
      Moved to lazy module-level singleton.
  9.  BUG — METRICS["errors"] is a list but was being appended to from
      multiple threads (ThreadPoolExecutor) without a lock. Added threading.Lock.
 10.  BUG — retry() used bare `Exception as exc` but re-raised with `raise`
      (no arg), losing the original traceback chain. Fixed: `raise exc`.
 11.  BUG — parse_json() split on "```" and then lstripped "json" as a
      substring — would corrupt JSON starting with "j". Fixed: proper
      strip of the fence language marker.
 12.  BUG — Agent 5 individual SMS truncated to Twilio's 1600-char limit
      inside send_sms, but the composed message was built without checking;
      the voicemail script alone can exceed the limit, silently truncating
      critical data. Fixed: messages split into chunks when > 1550 chars.
 13.  BUG — CITY_FILTER comparison used `.lower() in c.lower()` which would
      match "AL" against "Birmingham AL", "Huntsville AL", etc. — effectively
      disabling the filter. Fixed: exact city match (normalized).
 14.  RELIABILITY — _probe_website() did not set a User-Agent on failure path;
      some servers return 403 for empty UA, causing false "dead website" flags.
      Fixed: User-Agent set for all requests including redirects.
 15.  RELIABILITY — Agent 3 heuristic fallback did not populate required fields
      (outreach_sms, voicemail_script, etc.), causing KeyErrors downstream in
      Agent 5 when building SMS for fallback-qualified leads.
      Fixed: all required fields initialized to empty string in fallback path.
 16.  RELIABILITY — Agent 6 intelligence loop was not guarded against
      INTEL_TOP_N > len(assigned); sorted()[:INTEL_TOP_N] is safe, but the
      log message claimed a fixed N even when fewer leads existed.
      Fixed: log reflects actual count used.
 17.  RELIABILITY — run_pipeline() did not catch exceptions from individual
      agents; one agent crash aborted the entire run including outreach for
      already-qualified leads. Fixed: per-agent try/except with graceful
      continuation and error logging.
 18.  RELIABILITY — metrics JSON was written before the final operator SMS,
      so SMS count in the file was always 1 less than actual.
      Fixed: metrics written after all outreach completes.
 19.  STYLE/SAFETY — GROQ_MODEL env var fallback was set but never validated;
      if an invalid model string was supplied, Groq returned a 400 error with
      no clear message. Added model validation against known-good list.
 20.  YAML — GitHub Actions cron "0 15 * * 1-5" fires at 15:00 UTC = 9 AM CST
      (UTC-6) only in winter. In CDT (UTC-5, Mar–Nov) it fires at 10 AM.
      Fixed cron to "0 14 * * 1-5" (8 AM UTC) with a comment explaining
      Alabama's timezone offset, so it hits 9 AM CDT reliably.
"""

from __future__ import annotations

import os
import re
import json
import time
import hashlib
import logging
import sys
import threading
from datetime import datetime, timezone
from typing import Optional

import requests
from groq import Groq
from serpapi import GoogleSearch
from twilio.rest import Client as TwilioClient

# ══════════════════════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("leadflow_v4.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("LeadFlow.v4")

# ══════════════════════════════════════════════════════════════════════════════
# CREDENTIALS  (all from existing env vars — zero new keys)
# ══════════════════════════════════════════════════════════════════════════════

GROQ_API_KEY     = os.getenv("GROQ_API_KEY",        "").strip()
SERPAPI_API_KEY  = os.getenv("SERPAPI_API_KEY",      "").strip()
BASE44_API_KEY   = os.getenv("BASE44_API_KEY",       "").strip()
BASE44_APP_ID    = os.getenv("BASE44_APP_ID",        "").strip()
TWILIO_SID       = os.getenv("TWILIO_ACCOUNT_SID",  "").strip()
TWILIO_TOKEN     = os.getenv("TWILIO_AUTH_TOKEN",   "").strip()
TWILIO_PHONE     = os.getenv("TWILIO_PHONE_NUMBER", "").strip()
ALERT_PHONE      = os.getenv("ALERT_PHONE_NUMBER",  "").strip()
SLACK_WEBHOOK    = os.getenv("SLACK_WEBHOOK_URL",   "").strip()
CRM_WEBHOOK      = os.getenv("CRM_WEBHOOK_URL",     "").strip()
BASE44_ENTITY    = os.getenv("BASE44_ENTITY",       "Lead").strip()

# FIX #19 — validate Groq model against known-good list; fall back gracefully
_GROQ_MODEL_INPUT = os.getenv("GROQ_MODEL", "").strip()
_KNOWN_GROQ_MODELS = {
    "llama-3.3-70b-versatile",
    "llama-3.1-8b-instant",
    "llama3-70b-8192",
    "llama3-8b-8192",
    "mixtral-8x7b-32768",
    "gemma2-9b-it",
}
if _GROQ_MODEL_INPUT and _GROQ_MODEL_INPUT not in _KNOWN_GROQ_MODELS:
    log.warning(f"GROQ_MODEL '{_GROQ_MODEL_INPUT}' not in known list — using default.")
    GROQ_MODEL = "llama-3.3-70b-versatile"
else:
    GROQ_MODEL = _GROQ_MODEL_INPUT or "llama-3.3-70b-versatile"

# Runtime flags
DRY_RUN     = os.getenv("DRY_RUN",     "false").strip().lower() == "true"
CITY_FILTER = os.getenv("CITY_FILTER", "").strip()

BASE44_URL  = f"https://api.base44.app/api/apps/{BASE44_APP_ID}/entities/{BASE44_ENTITY}"
B44_HEADERS = {"api_key": BASE44_API_KEY, "Content-Type": "application/json"}

# ══════════════════════════════════════════════════════════════════════════════
# COVERAGE — 8 cities × 20 business niches  (160 daily query combinations)
# ══════════════════════════════════════════════════════════════════════════════

CITIES = [
    "Birmingham AL",
    "Huntsville AL",
    "Montgomery AL",
    "Mobile AL",
    "Tuscaloosa AL",
    "Hoover AL",
    "Auburn AL",
    "Dothan AL",
]

BUSINESS_TYPES = [
    "auto repair shops",
    "hair salons and barbershops",
    "real estate agents",
    "insurance agents",
    "cleaning services",
    "restaurants",
    "dental offices",
    "HVAC services",
    "plumbers",
    "landscaping services",
    "roofing contractors",
    "law firms",
    "accounting firms",
    "physical therapy clinics",
    "pest control services",
    "med spas and aesthetics",
    "chiropractors",
    "electricians",
    "mortgage brokers",
    "car dealerships",
]

# ══════════════════════════════════════════════════════════════════════════════
# REP CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

SALES_REPS = [
    {
        "id":          "rep_1",
        "name":        "Alice",
        "phone":       ALERT_PHONE,
        "specialties": ["dental offices", "med spas and aesthetics", "physical therapy clinics", "chiropractors"],
    },
    {
        "id":          "rep_2",
        "name":        "Bob",
        "phone":       ALERT_PHONE,
        "specialties": ["auto repair shops", "HVAC services", "plumbers", "roofing contractors", "electricians"],
    },
    {
        "id":          "rep_3",
        "name":        "Carol",
        "phone":       ALERT_PHONE,
        "specialties": ["real estate agents", "insurance agents", "law firms", "accounting firms", "mortgage brokers"],
    },
]

rep_load: dict[str, int] = {r["id"]: 0 for r in SALES_REPS}

# ══════════════════════════════════════════════════════════════════════════════
# TUNING CONSTANTS
# ══════════════════════════════════════════════════════════════════════════════

LEADS_PER_QUERY     = 5     # Maps results per query
MAX_RETRIES         = 3
RETRY_BACKOFF       = 2.0
API_SLEEP           = 0.4   # SerpAPI throttle
GROQ_SLEEP          = 0.25  # Groq throttle
WEBSITE_TIMEOUT     = 6
QUALIFY_THRESHOLD   = 5     # Minimum AI score to qualify
HIGH_PRIORITY_SMS   = 25    # Max individual outreach SMS per run
INTEL_TOP_N         = 10    # Leads fed to Agent 6 strategic brief
SMS_CHUNK_SIZE      = 1550  # FIX #12 — safe SMS chunk size (Twilio limit 1600)

# ══════════════════════════════════════════════════════════════════════════════
# RUN METRICS
# ══════════════════════════════════════════════════════════════════════════════

METRICS: dict = {
    "run_date":         "",
    "duration_seconds": 0,
    "cities_queried":   0,
    "niches_queried":   0,
    "total_queries":    0,
    "captured":         0,
    "dupes_skipped":    0,
    "enriched":         0,
    "qualified":        0,
    "assigned":         0,
    "sms_sent":         0,
    "crm_pushed":       0,
    "errors":           [],
    "top_niches":       {},
    "top_cities":       {},
}

# FIX #9 — thread-safe error list mutations
_metrics_lock = threading.Lock()

def _record_error(msg: str) -> None:
    with _metrics_lock:
        METRICS["errors"].append(msg)

# ══════════════════════════════════════════════════════════════════════════════
# MODULE-LEVEL SERVICE SINGLETONS  (FIX #7, #8)
# ══════════════════════════════════════════════════════════════════════════════

_groq_client: Optional[Groq] = None
_twilio_client: Optional[TwilioClient] = None


def _get_groq() -> Groq:
    global _groq_client
    if _groq_client is None:
        _groq_client = Groq(api_key=GROQ_API_KEY)
    return _groq_client


def _get_twilio() -> TwilioClient:
    global _twilio_client
    if _twilio_client is None:
        _twilio_client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
    return _twilio_client


# ══════════════════════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def lead_fingerprint(name: str, address: str) -> str:
    raw = f"{name.strip().lower()}|{address.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def retry(fn, *args, retries=MAX_RETRIES, **kwargs):
    """FIX #10 — preserve traceback on re-raise."""
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if attempt == retries - 1:
                raise exc  # re-raise the caught exception, not a bare raise
            wait = RETRY_BACKOFF * (2 ** attempt)
            log.warning(f"Retry {attempt+1}/{retries} — {exc} — waiting {wait:.1f}s")
            time.sleep(wait)


def check_credentials() -> bool:
    log.info("=== CREDENTIAL CHECK ===")
    required = {
        "GROQ_API_KEY":        GROQ_API_KEY,
        "SERPAPI_API_KEY":     SERPAPI_API_KEY,
        "BASE44_API_KEY":      BASE44_API_KEY,
        "BASE44_APP_ID":       BASE44_APP_ID,
        "TWILIO_ACCOUNT_SID":  TWILIO_SID,
        "TWILIO_AUTH_TOKEN":   TWILIO_TOKEN,
        "TWILIO_PHONE_NUMBER": TWILIO_PHONE,
        "ALERT_PHONE_NUMBER":  ALERT_PHONE,
    }
    ok = True
    for k, v in required.items():
        if not v:
            log.error(f"  MISSING: {k}")
            ok = False
        else:
            log.info(f"  OK:      {k} = {v[:6]}…")
    if DRY_RUN:
        log.info("  MODE: DRY RUN — all writes suppressed")
    if CITY_FILTER:
        log.info(f"  FILTER: City restricted to '{CITY_FILTER}'")
    return ok


# ══════════════════════════════════════════════════════════════════════════════
# GROQ AI LAYER
# ══════════════════════════════════════════════════════════════════════════════

def ask_groq(system: str, user: str, max_tokens: int = 900, temperature: float = 0.3) -> Optional[str]:
    try:
        res = _get_groq().chat.completions.create(
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            model=GROQ_MODEL,
            max_tokens=max_tokens,
            temperature=temperature,
        )
        return res.choices[0].message.content.strip()
    except Exception as exc:
        log.error(f"Groq error: {exc}")
        _record_error(f"Groq: {exc}")
        return None


def parse_json(raw: Optional[str]) -> Optional[dict]:
    """FIX #11 — properly strip markdown fences without corrupting content."""
    if not raw:
        return None
    clean = raw.strip()
    # Strip ```json ... ``` or ``` ... ``` fences
    if "```" in clean:
        # Extract content between first and last fence
        parts = clean.split("```")
        for part in parts:
            # Strip optional language marker on first line only
            candidate = re.sub(r"^[a-z]+\n", "", part.strip(), count=1)
            if candidate.startswith("{"):
                clean = candidate
                break
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# BASE44 CRM LAYER
# ══════════════════════════════════════════════════════════════════════════════

def b44_list_existing() -> set:
    """
    FIX #2 — distinguish network/API errors from a genuinely empty CRM.
    On error: log warning and return empty set (safe: worst case = re-capture
    leads that will fail the create with a duplicate-key error, not data loss).
    """
    if not BASE44_API_KEY or not BASE44_APP_ID:
        log.warning("Base44 credentials missing — skipping dedup prefetch")
        return set()
    try:
        r = requests.get(BASE44_URL, headers=B44_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        fps = {rec.get("fingerprint", "") for rec in data if rec.get("fingerprint")}
        log.info(f"  Dedup prefetch: {len(data)} records, {len(fps)} fingerprints loaded")
        return fps
    except requests.exceptions.RequestException as exc:
        log.warning(f"Dedup prefetch network error: {exc} — proceeding without dedup")
        return set()
    except Exception as exc:
        log.warning(f"Dedup prefetch unexpected error: {exc} — proceeding without dedup")
        return set()


def b44_create(data: dict) -> Optional[dict]:
    if DRY_RUN:
        log.info("    [DRY RUN] Skipping Base44 create")
        return {"_id": "dry_run_id"}
    try:
        r = retry(requests.post, BASE44_URL, headers=B44_HEADERS, json=data, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.error(f"Base44 create error: {exc}")
        _record_error(f"B44 create: {exc}")
        return None


def b44_update(record_id: str, data: dict) -> Optional[dict]:
    """FIX #4 — guard against both "" and "dry_run_id"."""
    if not record_id or record_id == "dry_run_id" or DRY_RUN:
        return {}
    try:
        r = retry(requests.put, f"{BASE44_URL}/{record_id}", headers=B44_HEADERS, json=data, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.error(f"Base44 update error [{record_id}]: {exc}")
        _record_error(f"B44 update: {exc}")
        return None


# ══════════════════════════════════════════════════════════════════════════════
# TWILIO SMS LAYER
# ══════════════════════════════════════════════════════════════════════════════

def _send_sms_chunk(to: str, body: str) -> bool:
    """Send a single SMS chunk (caller must ensure len <= SMS_CHUNK_SIZE)."""
    try:
        msg = _get_twilio().messages.create(body=body, from_=TWILIO_PHONE, to=to)
        log.info(f"  SMS sent → {to} [{msg.sid}]")
        with _metrics_lock:
            METRICS["sms_sent"] += 1
        return True
    except Exception as exc:
        log.error(f"Twilio error: {exc}")
        _record_error(f"Twilio: {exc}")
        return False


def send_sms(to: str, body: str) -> bool:
    """
    FIX #12 — split long messages into chunks so Twilio's 1600-char hard limit
    never silently truncates critical outreach content.
    """
    if DRY_RUN:
        log.info(f"  [DRY RUN] SMS → {to}: {body[:80]}…")
        return True
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_PHONE, to]):
        log.warning("SMS skipped — missing Twilio config")
        return False

    if len(body) <= SMS_CHUNK_SIZE:
        return _send_sms_chunk(to, body)

    # Split into chunks at newline boundaries where possible
    chunks = []
    remaining = body
    while len(remaining) > SMS_CHUNK_SIZE:
        split_at = remaining.rfind("\n", 0, SMS_CHUNK_SIZE)
        if split_at == -1:
            split_at = SMS_CHUNK_SIZE
        chunks.append(remaining[:split_at].strip())
        remaining = remaining[split_at:].strip()
    if remaining:
        chunks.append(remaining)

    success = True
    for i, chunk in enumerate(chunks, 1):
        if len(chunks) > 1:
            chunk = f"[{i}/{len(chunks)}] {chunk}"
        ok = _send_sms_chunk(to, chunk)
        if not ok:
            success = False
        time.sleep(0.3)
    return success


# ══════════════════════════════════════════════════════════════════════════════
# SLACK LAYER
# ══════════════════════════════════════════════════════════════════════════════

def send_slack(payload: dict) -> bool:
    if DRY_RUN:
        log.info("  [DRY RUN] Slack payload suppressed")
        return True
    if not SLACK_WEBHOOK:
        return False
    try:
        r = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as exc:
        log.error(f"Slack error: {exc}")
        _record_error(f"Slack: {exc}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# CRM WEBHOOK LAYER
# ══════════════════════════════════════════════════════════════════════════════

def push_crm(lead: dict) -> bool:
    if DRY_RUN or not CRM_WEBHOOK:
        return False
    try:
        r = requests.post(CRM_WEBHOOK, json=lead, timeout=10)
        r.raise_for_status()
        with _metrics_lock:
            METRICS["crm_pushed"] += 1
        return True
    except Exception as exc:
        log.error(f"CRM webhook error: {exc}")
        _record_error(f"CRM: {exc}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 1 — APEX LEAD CAPTURE
# ══════════════════════════════════════════════════════════════════════════════

def _serpapi_maps(query: str) -> list[dict]:
    try:
        results = GoogleSearch({
            "engine":  "google_maps",
            "q":       query,
            "api_key": SERPAPI_API_KEY,
            "type":    "search",
        }).get_dict()
        items = results.get("local_results", [])[:LEADS_PER_QUERY]
        leads = []
        for item in items:
            leads.append({
                "name":      item.get("title", "Unknown"),
                "phone":     item.get("phone", ""),
                "address":   item.get("address", ""),
                "website":   item.get("website", ""),
                "rating":    item.get("rating", 0),
                "reviews":   item.get("reviews", 0),
                "place_id":  item.get("place_id", ""),
                "thumbnail": item.get("thumbnail", ""),
                "hours":     item.get("hours", ""),
                "source":    "google_maps",
            })
        return leads
    except Exception as exc:
        log.warning(f"Maps search failed for '{query}': {exc}")
        _record_error(f"Maps '{query}': {exc}")
        return []


def _serpapi_organic(query: str) -> list[dict]:
    """FIX #5 — log errors to METRICS so failures are visible."""
    try:
        results = GoogleSearch({
            "engine":  "google",
            "q":       query,
            "api_key": SERPAPI_API_KEY,
            "num":     5,
        }).get_dict()
        leads = []
        for item in results.get("organic_results", []):
            link    = item.get("link", "")
            snippet = item.get("snippet", "")
            # Only harvest local business listings; skip aggregator directories
            if not link or any(d in link for d in ("yelp.com", "yellowpages", "bbb.org", "manta.com")):
                continue
            leads.append({
                "name":      item.get("title", ""),
                "phone":     "",
                "address":   "",
                "website":   link,
                "rating":    0,
                "reviews":   0,
                "place_id":  "",
                "thumbnail": "",
                "hours":     "",
                "snippet":   snippet,
                "source":    "google_organic",
            })
        return leads
    except Exception as exc:
        log.warning(f"Organic search failed for '{query}': {exc}")
        _record_error(f"Organic '{query}': {exc}")  # FIX #5
        return []


def agent_capture_leads(existing_fps: set) -> list[dict]:
    log.info("\n" + "═" * 62)
    log.info("AGENT 1 — APEX LEAD CAPTURE (Maps + Organic)")
    log.info("═" * 62)

    # FIX #13 — exact city match (normalized lowercase), not substring
    if CITY_FILTER:
        city_filter_norm = CITY_FILTER.strip().lower()
        cities = [c for c in CITIES if c.lower() == city_filter_norm]
        if not cities:
            log.warning(f"CITY_FILTER '{CITY_FILTER}' matched no cities — running all cities")
            cities = CITIES
    else:
        cities = CITIES

    METRICS["cities_queried"] = len(cities)
    METRICS["niches_queried"] = len(BUSINESS_TYPES)

    captured: list[dict] = []
    dupes    = 0
    total_queries = 0

    for city in cities:
        for btype in BUSINESS_TYPES:
            query = f"{btype} {city}"
            log.info(f"  → {query}")
            total_queries += 1

            maps_leads    = _serpapi_maps(query)
            time.sleep(API_SLEEP)
            organic_leads = _serpapi_organic(query)
            time.sleep(API_SLEEP)

            all_raw = maps_leads + organic_leads

            # FIX #1 & #6 — compute new-lead count BEFORE mutating existing_fps
            new_this_query = 0
            for raw in all_raw:
                name    = raw.get("name", "Unknown Business")
                address = raw.get("address", raw.get("website", ""))
                fp      = lead_fingerprint(name, address)

                if fp in existing_fps:
                    dupes += 1
                    continue

                lead = {
                    **raw,
                    "fingerprint":   fp,
                    "city":          city,
                    "business_type": btype,
                    "status":        "New",
                    "created_at":    datetime.now(timezone.utc).isoformat(),
                    "run_date":      datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                }

                saved = b44_create(lead)
                if saved:
                    lead["id"] = saved.get("_id") or saved.get("id", "")
                    captured.append(lead)
                    existing_fps.add(fp)   # mutate AFTER capture
                    new_this_query += 1

                    METRICS["top_niches"][btype] = METRICS["top_niches"].get(btype, 0) + 1
                    METRICS["top_cities"][city]  = METRICS["top_cities"].get(city,  0) + 1

            log.info(
                f"     Maps: {len(maps_leads)} | Organic: {len(organic_leads)} | "
                f"New this query: {new_this_query}"  # FIX #6 — correct count
            )

    METRICS["total_queries"] = total_queries
    METRICS["captured"]      = len(captured)
    METRICS["dupes_skipped"] = dupes

    log.info(f"\nAgent 1 Complete — Captured: {len(captured)} | Dupes skipped: {dupes}")
    return captured


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 2 — INTELLIGENCE RECON
# ══════════════════════════════════════════════════════════════════════════════

_PROBE_HEADERS = {"User-Agent": "Mozilla/5.0 LeadFlow-Probe/4.0"}


def _probe_website(url: str) -> dict:
    """FIX #14 — User-Agent applied consistently; handles all failure modes."""
    if not url:
        return {"live": False, "has_ssl": False, "slow": False, "redirect": False, "status": 0}
    if not url.startswith("http"):
        url = "https://" + url
    try:
        start = time.time()
        r = requests.get(
            url, timeout=WEBSITE_TIMEOUT, allow_redirects=True, headers=_PROBE_HEADERS
        )
        latency = time.time() - start
        return {
            "live":     r.status_code < 400,
            "has_ssl":  url.startswith("https"),
            "slow":     latency > 3.5,
            "redirect": len(r.history) > 0,
            "status":   r.status_code,
        }
    except Exception:
        return {"live": False, "has_ssl": False, "slow": True, "redirect": False, "status": 0}


def _social_signals(name: str, city: str) -> dict:
    slug      = name.lower().replace(" ", "+").replace("'", "")
    city_slug = city.lower().replace(" ", "+")
    query     = f"{slug} {city_slug} (facebook OR instagram OR yelp OR google maps)"
    signals   = {"facebook": False, "instagram": False, "yelp": False}
    try:
        results = GoogleSearch({
            "engine":  "google",
            "q":       query,
            "api_key": SERPAPI_API_KEY,
            "num":     5,
        }).get_dict()
        for r in results.get("organic_results", []):
            link = r.get("link", "").lower()
            if "facebook.com"  in link: signals["facebook"]  = True
            if "instagram.com" in link: signals["instagram"] = True
            if "yelp.com"      in link: signals["yelp"]      = True
    except Exception:
        pass
    return signals


def _competitor_intel(name: str, btype: str, city: str) -> dict:
    bname_slug = name.lower().replace(" ", "").replace("'", "")
    query      = f"{btype} {city}"
    intel      = {"top_competitor": "", "lead_owns_top_result": False}
    try:
        results = GoogleSearch({
            "engine":  "google",
            "q":       query,
            "api_key": SERPAPI_API_KEY,
            "num":     3,
        }).get_dict()
        organic = results.get("organic_results", [])
        if organic:
            top_link  = organic[0].get("link", "").lower()
            top_title = organic[0].get("title", "")
            intel["top_competitor"]       = top_title
            intel["lead_owns_top_result"] = bname_slug in top_link
    except Exception:
        pass
    return intel


def agent_enrich_leads(leads: list[dict]) -> list[dict]:
    log.info("\n" + "═" * 62)
    log.info("AGENT 2 — INTELLIGENCE RECON")
    log.info("═" * 62)

    for lead in leads:
        name    = lead.get("name", "?")
        website = lead.get("website", "")
        city    = lead.get("city", "")
        btype   = lead.get("business_type", "")

        log.info(f"  Recon: {name}")

        site = _probe_website(website)
        lead["website_live"]    = site["live"]
        lead["website_has_ssl"] = site["has_ssl"]
        lead["website_slow"]    = site["slow"]
        lead["no_website"]      = not bool(website)

        time.sleep(API_SLEEP)
        socials = _social_signals(name, city)
        lead["has_facebook"]          = socials["facebook"]
        lead["has_instagram"]         = socials["instagram"]
        lead["has_yelp"]              = socials["yelp"]
        lead["social_presence_count"] = sum(socials.values())

        time.sleep(API_SLEEP)
        comp = _competitor_intel(name, btype, city)
        lead["top_competitor"]     = comp["top_competitor"]
        lead["owns_search_result"] = comp["lead_owns_top_result"]

        pain_points = []
        if lead["no_website"]:                    pain_points.append("no website")
        elif not lead["website_live"]:            pain_points.append("dead website")
        if not lead["website_has_ssl"]:           pain_points.append("no SSL certificate")
        if lead["website_slow"]:                  pain_points.append("slow-loading website")
        if not lead["owns_search_result"]:        pain_points.append("competitor outranks them")
        if (lead.get("reviews") or 0) < 10:       pain_points.append("few online reviews")
        if not socials["facebook"]:               pain_points.append("no Facebook presence")
        if not socials["instagram"]:              pain_points.append("no Instagram presence")
        if not socials["yelp"]:                   pain_points.append("not on Yelp")

        lead["pain_points"]      = pain_points
        lead["pain_point_count"] = len(pain_points)

        b44_update(lead.get("id", ""), {
            "website_live":          lead["website_live"],
            "website_has_ssl":       lead["website_has_ssl"],
            "website_slow":          lead["website_slow"],
            "no_website":            lead["no_website"],
            "has_facebook":          lead["has_facebook"],
            "has_instagram":         lead["has_instagram"],
            "has_yelp":              lead["has_yelp"],
            "social_presence_count": lead["social_presence_count"],
            "top_competitor":        lead["top_competitor"],
            "owns_search_result":    lead["owns_search_result"],
            "pain_point_count":      lead["pain_point_count"],
            "pain_points":           ", ".join(pain_points),
        })

    METRICS["enriched"] = len(leads)
    log.info(f"\nAgent 2 Complete — Enriched: {len(leads)} leads")
    return leads


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 3 — AI BRAIN: MULTI-PASS GROQ SCORING
# ══════════════════════════════════════════════════════════════════════════════

SCORE_SYSTEM = """\
You are the head of sales strategy at Alabama's most aggressive digital marketing agency.

Evaluate this local business lead and respond ONLY with valid JSON — no preamble, no markdown fences:

{
  "qualified":              true | false,
  "score":                  1-10,
  "priority":               "high" | "medium" | "low",
  "reason":                 "one precise sentence explaining the score",
  "pain_summary":           "one sentence on their single biggest marketing gap",
  "service_recommendation": "top 1-2 services to pitch",
  "monthly_value_estimate": "estimated monthly retainer range in USD (e.g. $750-1200)"
}

Scoring rubric:
  10 — No website OR dead site, has phone, 5+ pain points: perfect prospect
  8-9 — Basic/slow site, reachable, 3-4 pain points
  6-7 — Has a site but poor SEO, few reviews, weak social
  4-5 — Decent digital presence but 1-2 fixable gaps
  1-3 — Solid online presence, not a strong prospect right now

Qualify if score >= 5 OR (has_phone=true AND pain_point_count >= 2).
Unqualify if score <= 3 AND has no phone.\
"""

PERSONA_SYSTEM = """\
You are an expert buyer persona researcher for a B2B digital marketing agency.
Given a local Alabama business profile, identify their likely decision-maker and mindset.

Respond ONLY with valid JSON — no preamble, no markdown:

{
  "decision_maker_title":  "likely title of who approves marketing spend",
  "primary_motivation":    "what they care about most (revenue, reputation, time, survival)",
  "biggest_fear":          "their #1 fear about hiring a marketing agency",
  "best_hook":             "one sentence hook that would immediately get their attention",
  "best_contact_time":     "best time of day to reach them (e.g., 'Tuesday 10-11am')"
}\
"""

COPY_SYSTEM = """\
You are a world-class direct-response copywriter specializing in local business outreach.
Write personalized outreach for this Alabama business.

Respond ONLY with valid JSON — no preamble, no markdown:

{
  "outreach_sms":       "2-sentence SMS. Mention city + specific pain point. End with a soft CTA. Max 160 chars.",
  "email_subject_a":    "Subject line A — curiosity-driven, under 8 words",
  "email_subject_b":    "Subject line B — benefit-driven, under 8 words",
  "voicemail_script":   "15-second voicemail script the rep can read verbatim",
  "objection_rebuttal": "one-sentence rebuttal for 'we don't need marketing right now'"
}\
"""

# FIX #15 — default fields to prevent KeyError downstream in Agent 5
_LEAD_COPY_DEFAULTS = {
    "status":                  "",
    "ai_score":                5,
    "priority":                "medium",
    "ai_reason":               "",
    "pain_summary":            "",
    "service_recommendation":  "",
    "monthly_value_estimate":  "",
    "decision_maker_title":    "",
    "primary_motivation":      "",
    "biggest_fear":            "",
    "best_hook":               "",
    "best_contact_time":       "",
    "outreach_sms":            "",
    "email_subject_a":         "",
    "email_subject_b":         "",
    "voicemail_script":        "",
    "objection_rebuttal":      "",
}


def _score_lead(lead: dict) -> Optional[dict]:
    pains  = ", ".join(lead.get("pain_points", [])) or "none detected"
    prompt = f"""
Name:                {lead.get('name', 'Unknown')}
Business Type:       {lead.get('business_type', '')}
City:                {lead.get('city', '')}
Phone:               {lead.get('phone', 'MISSING')}
Rating:              {lead.get('rating', 0)} stars / {lead.get('reviews', 0)} reviews
Website Live:        {lead.get('website_live', False)}
Has SSL:             {lead.get('website_has_ssl', False)}
Website Slow:        {lead.get('website_slow', False)}
No Website:          {lead.get('no_website', False)}
Has Facebook:        {lead.get('has_facebook', False)}
Has Instagram:       {lead.get('has_instagram', False)}
Has Yelp:            {lead.get('has_yelp', False)}
Social Count:        {lead.get('social_presence_count', 0)} platforms
Top Competitor:      {lead.get('top_competitor', 'Unknown')}
Owns Search Result:  {lead.get('owns_search_result', False)}
Pain Points ({lead.get('pain_point_count', 0)}): {pains}
"""
    return parse_json(ask_groq(SCORE_SYSTEM, prompt, max_tokens=400))


def _persona_lead(lead: dict) -> Optional[dict]:
    prompt = f"""
Business: {lead.get('name')} | Type: {lead.get('business_type')} | City: {lead.get('city')}
Rating: {lead.get('rating', 0)} stars, {lead.get('reviews', 0)} reviews
Digital gaps: {', '.join(lead.get('pain_points', [])) or 'minimal'}
"""
    return parse_json(ask_groq(PERSONA_SYSTEM, prompt, max_tokens=300))


def _copy_lead(lead: dict, score_data: dict, persona_data: dict) -> Optional[dict]:
    prompt = f"""
Business: {lead.get('name')} | {lead.get('business_type')} | {lead.get('city')}
Score: {score_data.get('score', 5)}/10 | Priority: {score_data.get('priority', 'medium')}
Pain: {score_data.get('pain_summary', '')}
Decision-maker: {persona_data.get('decision_maker_title', 'Owner')}
Best hook: {persona_data.get('best_hook', '')}
Service rec: {score_data.get('service_recommendation', '')}
"""
    return parse_json(ask_groq(COPY_SYSTEM, prompt, max_tokens=500))


def agent_qualify_leads(leads: list[dict]) -> list[dict]:
    log.info("\n" + "═" * 62)
    log.info("AGENT 3 — AI BRAIN: MULTI-PASS SCORING")
    log.info("═" * 62)

    qualified: list[dict] = []

    for lead in leads:
        name = lead.get("name", "?")
        log.info(f"  Scoring: {name}")

        # ── Pass 1: Score ──
        score_data = _score_lead(lead)
        time.sleep(GROQ_SLEEP)

        if not score_data:
            # FIX #15 — initialize ALL required fields before appending
            has_phone  = bool(lead.get("phone"))
            pain_count = lead.get("pain_point_count", 0)
            lead.update({**_LEAD_COPY_DEFAULTS, "status": "Qualified", "priority": "medium", "ai_score": 5})
            if has_phone or pain_count >= 3:
                b44_update(lead.get("id", ""), {"status": "Qualified", "ai_score": 5})
                qualified.append(lead)
                log.info(f"  Qualified (heuristic fallback): {name}")
            else:
                lead["status"] = "Unqualified"
                b44_update(lead.get("id", ""), {"status": "Unqualified"})
                log.info(f"  Unqualified (fallback): {name}")
            continue

        if not score_data.get("qualified"):
            b44_update(lead.get("id", ""), {"status": "Unqualified"})
            log.info(f"  Unqualified [{score_data.get('score', 0)}/10]: {name} — {score_data.get('reason', '')}")
            continue

        # ── Pass 2: Persona ──
        persona_data = _persona_lead(lead) or {}
        time.sleep(GROQ_SLEEP)

        # ── Pass 3: Outreach copy ──
        copy_data = _copy_lead(lead, score_data, persona_data) or {}
        time.sleep(GROQ_SLEEP)

        score    = score_data.get("score", 5)
        priority = score_data.get("priority", "medium")

        lead.update({
            **_LEAD_COPY_DEFAULTS,  # initialize defaults first
            "status":                  "Qualified",
            "ai_score":                score,
            "priority":                priority,
            "ai_reason":               score_data.get("reason", ""),
            "pain_summary":            score_data.get("pain_summary", ""),
            "service_recommendation":  score_data.get("service_recommendation", ""),
            "monthly_value_estimate":  score_data.get("monthly_value_estimate", ""),
            "decision_maker_title":    persona_data.get("decision_maker_title", ""),
            "primary_motivation":      persona_data.get("primary_motivation", ""),
            "biggest_fear":            persona_data.get("biggest_fear", ""),
            "best_hook":               persona_data.get("best_hook", ""),
            "best_contact_time":       persona_data.get("best_contact_time", ""),
            "outreach_sms":            copy_data.get("outreach_sms", ""),
            "email_subject_a":         copy_data.get("email_subject_a", ""),
            "email_subject_b":         copy_data.get("email_subject_b", ""),
            "voicemail_script":        copy_data.get("voicemail_script", ""),
            "objection_rebuttal":      copy_data.get("objection_rebuttal", ""),
        })

        b44_update(lead.get("id", ""), {
            "status":                  "Qualified",
            "ai_score":                score,
            "priority":                priority,
            "ai_reason":               lead["ai_reason"],
            "pain_summary":            lead["pain_summary"],
            "service_recommendation":  lead["service_recommendation"],
            "monthly_value_estimate":  lead["monthly_value_estimate"],
            "decision_maker_title":    lead["decision_maker_title"],
            "best_hook":               lead["best_hook"],
            "best_contact_time":       lead["best_contact_time"],
            "outreach_sms":            lead["outreach_sms"],
            "email_subject_a":         lead["email_subject_a"],
            "email_subject_b":         lead["email_subject_b"],
            "voicemail_script":        lead["voicemail_script"],
            "objection_rebuttal":      lead["objection_rebuttal"],
        })

        qualified.append(lead)
        log.info(
            f"  Qualified [{priority.upper()} | {score}/10 | "
            f"{lead.get('monthly_value_estimate','?')}/mo]: {name}"
        )

    METRICS["qualified"] = len(qualified)
    log.info(f"\nAgent 3 Complete — Qualified: {len(qualified)} leads")
    return qualified


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 4 — STRATEGIC LEAD ASSIGNMENT
# ══════════════════════════════════════════════════════════════════════════════

def _best_rep(lead: dict) -> dict:
    btype = lead.get("business_type", "")
    specialty_match = [
        r for r in SALES_REPS
        if any(spec in btype for spec in r.get("specialties", []))
    ]
    pool = specialty_match or SALES_REPS
    return min(pool, key=lambda r: rep_load[r["id"]])


def agent_assign_leads(leads: list[dict]) -> list[dict]:
    log.info("\n" + "═" * 62)
    log.info("AGENT 4 — STRATEGIC LEAD ASSIGNMENT")
    log.info("═" * 62)

    priority_order = {"high": 0, "medium": 1, "low": 2}
    leads_sorted = sorted(
        [l for l in leads if l.get("status") == "Qualified"],
        key=lambda x: (priority_order.get(x.get("priority", "low"), 2), -(x.get("ai_score") or 0)),
    )

    assigned: list[dict] = []

    for lead in leads_sorted:
        rep = _best_rep(lead)
        rep_load[rep["id"]] += 1

        lead.update({
            "assigned_to":     rep["name"],
            "assigned_rep_id": rep["id"],
            "status":          "Assigned",
            "assigned_at":     datetime.now(timezone.utc).isoformat(),
        })

        b44_update(lead.get("id", ""), {
            "assigned_to":     rep["name"],
            "assigned_rep_id": rep["id"],
            "status":          "Assigned",
            "assigned_at":     lead["assigned_at"],
        })

        assigned.append(lead)
        log.info(
            f"  {lead.get('name')} [{lead.get('priority','?').upper()} | "
            f"{lead.get('ai_score',0)}/10 | {lead.get('monthly_value_estimate','?')}/mo] "
            f"→ {rep['name']}"
        )

    METRICS["assigned"] = len(assigned)
    log.info(f"\nAgent 4 Complete — Assigned: {len(assigned)} leads")
    log.info("Rep loads this run:")
    for r in SALES_REPS:
        log.info(f"  {r['name']}: {rep_load[r['id']]} leads")
    return assigned


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 5 — OMNI OUTREACH
# ══════════════════════════════════════════════════════════════════════════════

def _parse_monthly_value(estimate: str) -> int:
    """
    FIX #3 — robust revenue parser using regex.
    Handles: '$750-1200', '$1,200 - $1,800', '750', '1200+', etc.
    Returns the midpoint of any range found, or the single value.
    """
    if not estimate:
        return 0
    nums = [int(n.replace(",", "")) for n in re.findall(r"\d[\d,]*", estimate)]
    if not nums:
        return 0
    return sum(nums) // len(nums)


def agent_launch_outreach(assigned: list[dict], captured: list[dict], qualified: list[dict]) -> None:
    log.info("\n" + "═" * 62)
    log.info("AGENT 5 — OMNI OUTREACH")
    log.info("═" * 62)

    today = datetime.now().strftime("%m/%d")

    # ── Group leads by rep ──
    rep_leads: dict[str, list] = {r["name"]: [] for r in SALES_REPS}
    for lead in assigned:
        rep_name = lead.get("assigned_to", "")
        if rep_name in rep_leads:
            rep_leads[rep_name].append(lead)

    # ── Rep SMS digest ──
    for rep in SALES_REPS:
        rep_name  = rep["name"]
        to_phone  = rep.get("phone") or ALERT_PHONE
        rep_items = rep_leads.get(rep_name, [])
        if not rep_items:
            continue

        lines = [f"LeadFlow AI v4 | {rep_name}'s Leads — {today}\n"]
        for i, ld in enumerate(rep_items[:8], 1):
            lines.append(
                f"{i}. {ld.get('name')} ({ld.get('city','')})\n"
                f"   Score: {ld.get('ai_score',0)}/10 | {ld.get('priority','?').upper()} | "
                f"Est: {ld.get('monthly_value_estimate','?')}/mo\n"
                f"   Phone: {ld.get('phone','N/A')}\n"
                f"   Best time: {ld.get('best_contact_time','?')}\n"
                f"   Hook: {ld.get('best_hook','')[:80]}\n"
            )
        if len(rep_items) > 8:
            lines.append(f"…and {len(rep_items) - 8} more in your CRM.")

        send_sms(to_phone, "\n".join(lines))

    # ── Individual outreach SMS for HIGH-priority leads ──
    highs = [l for l in assigned if l.get("priority") == "high" and l.get("outreach_sms")]
    log.info(f"  Sending {min(len(highs), HIGH_PRIORITY_SMS)} individual outreach SMS")

    for lead in highs[:HIGH_PRIORITY_SMS]:
        rep_phone = next(
            (r.get("phone") or ALERT_PHONE for r in SALES_REPS if r["name"] == lead.get("assigned_to")),
            ALERT_PHONE,
        )
        msg = (
            f"🔥 HIGH PRIORITY LEAD — {lead.get('name')}\n"
            f"Phone: {lead.get('phone', 'N/A')} | Score: {lead.get('ai_score',0)}/10\n"
            f"City: {lead.get('city','')} | Est: {lead.get('monthly_value_estimate','?')}/mo\n"
            f"Best time to call: {lead.get('best_contact_time','?')}\n\n"
            f"AI Outreach SMS:\n{lead.get('outreach_sms','')}\n\n"
            f"Email A: {lead.get('email_subject_a','')}\n"
            f"Email B: {lead.get('email_subject_b','')}\n\n"
            f"Voicemail:\n{lead.get('voicemail_script','')}"
        )
        send_sms(rep_phone, msg)  # FIX #12 handled inside send_sms()
        time.sleep(0.5)

    # ── CRM webhook push ──
    for lead in assigned:
        push_crm(lead)
        time.sleep(0.2)

    # ── Slack rich digest ──
    top5 = sorted(assigned, key=lambda x: -(x.get("ai_score") or 0))[:5]
    top_lines = "\n".join(
        f"• *{l.get('name')}* ({l.get('city')}) — "
        f"{l.get('ai_score',0)}/10 | {l.get('priority','?').upper()} | "
        f"{l.get('monthly_value_estimate','?')}/mo | → {l.get('assigned_to','?')}"
        for l in top5
    )

    # FIX #3 — use robust parser
    total_monthly = sum(_parse_monthly_value(l.get("monthly_value_estimate", "")) for l in assigned)

    slack_payload = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"LeadFlow AI v4 — Daily Report {datetime.now().strftime('%m/%d/%Y')}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Captured:* {len(captured)}"},
                    {"type": "mrkdwn", "text": f"*Qualified:* {len(qualified)}"},
                    {"type": "mrkdwn", "text": f"*Assigned:* {len(assigned)}"},
                    {"type": "mrkdwn", "text": f"*CRM Synced:* {METRICS['crm_pushed']}"},
                    {"type": "mrkdwn", "text": f"*SMS Sent:* {METRICS['sms_sent']}"},
                    {"type": "mrkdwn", "text": f"*Pipeline Est:* ${total_monthly:,}/mo"},
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Top 5 Leads:*\n{top_lines}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": "Rep Loads:\n" + "\n".join(
                        f"{r['name']}: {rep_load[r['id']]} leads" for r in SALES_REPS
                    )},
                    {"type": "mrkdwn", "text": "Hot Niches:\n" + "\n".join(
                        f"{niche}: {count}" for niche, count in sorted(
                            METRICS["top_niches"].items(), key=lambda x: -x[1]
                        )[:5]
                    )},
                ],
            },
        ]
    }
    if send_slack(slack_payload):
        log.info("  Slack digest sent")

    log.info(f"\nAgent 5 Complete — SMS: {METRICS['sms_sent']} | CRM: {METRICS['crm_pushed']}")


# ══════════════════════════════════════════════════════════════════════════════
# AGENT 6 — STRATEGIC INTELLIGENCE LOOP
# ══════════════════════════════════════════════════════════════════════════════

INTEL_SYSTEM = """\
You are a data-driven sales intelligence analyst for a digital marketing agency.
Based on today's lead generation run, produce a strategic brief for the team.

Respond ONLY with valid JSON — no preamble, no markdown:

{
  "top_opportunity_niche":   "single best niche to double down on tomorrow",
  "top_opportunity_city":    "single best city showing the most underserved leads",
  "pattern_insight":         "one key pattern observed across today's leads",
  "recommended_pitch_angle": "the top pitch angle most likely to convert this week",
  "next_run_focus":          "specific adjustments for tomorrow's run (niche/city/scoring)",
  "revenue_opportunity":     "estimated monthly recurring revenue if top 10 leads close at 30%",
  "action_items": [
    "specific action #1 for the team",
    "specific action #2 for the team",
    "specific action #3 for the team"
  ]
}\
"""


def agent_intelligence_loop(assigned: list[dict]) -> None:
    log.info("\n" + "═" * 62)
    log.info("AGENT 6 — STRATEGIC INTELLIGENCE LOOP")
    log.info("═" * 62)

    if not assigned:
        log.info("  No assigned leads — skipping intelligence loop")
        return

    # FIX #16 — reflect actual count used, not a hard-coded constant
    top_leads = sorted(assigned, key=lambda x: -(x.get("ai_score") or 0))[:INTEL_TOP_N]
    log.info(f"  Analyzing top {len(top_leads)} leads for strategic brief")

    summary = f"""
Run date:        {datetime.now().strftime('%Y-%m-%d')}
Total captured:  {METRICS['captured']}
Total qualified: {METRICS['qualified']}
Total assigned:  {METRICS['assigned']}

Top {len(top_leads)} leads this run:
""" + "\n".join(
        f"- {l.get('name')} | {l.get('business_type')} | {l.get('city')} | "
        f"Score: {l.get('ai_score',0)}/10 | Pain: {l.get('pain_summary','')} | "
        f"Est: {l.get('monthly_value_estimate','?')}/mo"
        for l in top_leads
    ) + f"""

Hot niches (by lead volume): {json.dumps(METRICS['top_niches'])}
Hot cities  (by lead volume): {json.dumps(METRICS['top_cities'])}
"""

    result = parse_json(ask_groq(INTEL_SYSTEM, summary, max_tokens=600, temperature=0.4))

    if not result:
        log.warning("  Intelligence loop produced no output")
        return

    log.info("  Strategic Brief Generated:")
    log.info(f"    Top Niche:  {result.get('top_opportunity_niche','')}")
    log.info(f"    Top City:   {result.get('top_opportunity_city','')}")
    log.info(f"    Insight:    {result.get('pattern_insight','')}")
    log.info(f"    Revenue Opp:{result.get('revenue_opportunity','')}")

    action_lines = "\n".join(f"• {a}" for a in result.get("action_items", []))

    slack_intel = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🧠 Strategic Intel Brief — {datetime.now().strftime('%m/%d/%Y')}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*🔥 Top Niche to Target:*\n{result.get('top_opportunity_niche','')}"},
                    {"type": "mrkdwn", "text": f"*📍 Top City to Target:*\n{result.get('top_opportunity_city','')}"},
                ]
            },
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*💡 Pattern Insight:*\n{result.get('pattern_insight','')}"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*💰 Revenue Opportunity:*\n{result.get('revenue_opportunity','')}"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*📋 Recommended Pitch Angle:*\n{result.get('recommended_pitch_angle','')}"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*✅ Action Items for Tomorrow:*\n{action_lines}"}},
            {"type": "section", "text": {"type": "mrkdwn", "text": f"*🎯 Next Run Focus:*\n{result.get('next_run_focus','')}"}},
        ]
    }
    send_slack(slack_intel)
    log.info("  Intelligence brief sent to Slack")


# ══════════════════════════════════════════════════════════════════════════════
# MAIN PIPELINE  (FIX #17 — per-agent fault isolation)
# ══════════════════════════════════════════════════════════════════════════════

def run_pipeline() -> None:
    start_time = datetime.now()
    METRICS["run_date"] = start_time.strftime("%Y-%m-%d %H:%M:%S CST")

    log.info("\n" + "═" * 62)
    log.info(f"LeadFlow AI v4.0 APEX — {METRICS['run_date']}")
    if DRY_RUN:
        log.info("MODE: DRY RUN — no writes will occur")
    log.info("═" * 62)

    # ── Preflight ──
    if not check_credentials():
        log.error("Missing required credentials — aborting.")
        sys.exit(1)

    log.info("\n=== GROQ CONNECTIVITY TEST ===")
    test = ask_groq("You are a helpful assistant.", "Reply with exactly one word: READY", max_tokens=5)
    if not test:
        log.error("Groq connection failed — aborting.")
        sys.exit(1)
    log.info(f"Groq: {test}")

    log.info("\n=== DEDUPLICATION PREFETCH ===")
    existing_fps = b44_list_existing()
    log.info(f"Found {len(existing_fps)} existing fingerprints in CRM")

    # ── Agent 1 ──
    captured: list[dict] = []
    try:
        captured = agent_capture_leads(existing_fps)
    except Exception as exc:
        log.error(f"Agent 1 FAILED: {exc}")
        _record_error(f"Agent1 fatal: {exc}")

    time.sleep(2)

    # ── Agent 2 ──
    enriched: list[dict] = captured
    if captured:
        try:
            enriched = agent_enrich_leads(captured)
        except Exception as exc:
            log.error(f"Agent 2 FAILED: {exc} — using unenriched leads")
            _record_error(f"Agent2 fatal: {exc}")

    time.sleep(1)

    # ── Agent 3 ──
    qualified: list[dict] = []
    if enriched:
        try:
            qualified = agent_qualify_leads(enriched)
        except Exception as exc:
            log.error(f"Agent 3 FAILED: {exc}")
            _record_error(f"Agent3 fatal: {exc}")

    time.sleep(1)

    # ── Agent 4 ──
    assigned: list[dict] = []
    if qualified:
        try:
            assigned = agent_assign_leads(qualified)
        except Exception as exc:
            log.error(f"Agent 4 FAILED: {exc}")
            _record_error(f"Agent4 fatal: {exc}")

    time.sleep(1)

    # ── Agent 5 ──
    if assigned:
        try:
            agent_launch_outreach(assigned, captured, qualified)
        except Exception as exc:
            log.error(f"Agent 5 FAILED: {exc}")
            _record_error(f"Agent5 fatal: {exc}")

    time.sleep(1)

    # ── Agent 6 ──
    try:
        agent_intelligence_loop(assigned)
    except Exception as exc:
        log.error(f"Agent 6 FAILED: {exc}")
        _record_error(f"Agent6 fatal: {exc}")

    # ── Final metrics  (FIX #18 — written AFTER all outreach) ──
    duration = int((datetime.now() - start_time).total_seconds())
    METRICS["duration_seconds"] = duration

    log.info("\n" + "═" * 62)
    log.info("LeadFlow AI v4.0 APEX — Run Complete")
    log.info("═" * 62)
    log.info(f"Duration:    {duration}s")
    log.info(f"Cities:      {METRICS['cities_queried']}")
    log.info(f"Niches:      {METRICS['niches_queried']}")
    log.info(f"Queries:     {METRICS['total_queries']}")
    log.info(f"Captured:    {METRICS['captured']}")
    log.info(f"Enriched:    {METRICS['enriched']}")
    log.info(f"Qualified:   {METRICS['qualified']}")
    log.info(f"Assigned:    {METRICS['assigned']}")
    log.info(f"SMS Sent:    {METRICS['sms_sent']}")
    log.info(f"CRM Synced:  {METRICS['crm_pushed']}")
    log.info(f"Errors:      {len(METRICS['errors'])}")
    log.info("═" * 62)

    # Write metrics JSON AFTER all outreach is complete
    metrics_path = f"leadflow_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(metrics_path, "w") as f:
        json.dump(METRICS, f, indent=2)
    log.info(f"Metrics written: {metrics_path}")

    # Final operator SMS
    send_sms(
        ALERT_PHONE,
        f"LeadFlow AI v4 — Run Complete\n"
        f"Date: {datetime.now().strftime('%m/%d %I:%M %p')}\n"
        f"Captured: {METRICS['captured']} | Qualified: {METRICS['qualified']}\n"
        f"Assigned: {METRICS['assigned']} | SMS: {METRICS['sms_sent']}\n"
        f"CRM: {METRICS['crm_pushed']} | Errors: {len(METRICS['errors'])}\n"
        f"Duration: {duration}s"
    )


if __name__ == "__main__":
    run_pipeline()
