"""
LeadFlow AI — v4.0 APEX
════════════════════════════════════════════════════════════════
Agent 1  APEX Lead Capture    Multi-engine SerpAPI sweep (Maps +
                               organic) across 8 cities × 20 niches
Agent 2  Intelligence Recon   Website probe, social signal mining,
                               competitor gap analysis
Agent 3  AI Brain Scoring     Groq multi-pass: score → persona →
                               outreach copy (3 calls per lead)
Agent 4  Strategic Assignment Skill-match reps by niche + load
                               balancing
Agent 5  Omni Outreach        Rep SMS digest + per-lead AI outreach
                               SMS + Slack digest + CRM webhook
Agent 6  Intelligence Loop    Groq synthesises run patterns, flags
                               top niches, generates next-run advice
════════════════════════════════════════════════════════════════
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import requests
from groq import Groq
from serpapi import GoogleSearch
from twilio.rest import Client as TwilioClient

# ══════════════════════════════════════════════════════════════
# LOGGING
# ══════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("leadflow_v4.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("LeadFlow.v4")

# ══════════════════════════════════════════════════════════════
# CREDENTIALS
# ══════════════════════════════════════════════════════════════

GROQ_API_KEY    = os.getenv("GROQ_API_KEY",        "").strip()
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY",      "").strip()
BASE44_API_KEY  = os.getenv("BASE44_API_KEY",       "").strip()
BASE44_APP_ID   = os.getenv("BASE44_APP_ID",        "").strip()
TWILIO_SID      = os.getenv("TWILIO_ACCOUNT_SID",  "").strip()
TWILIO_TOKEN    = os.getenv("TWILIO_AUTH_TOKEN",   "").strip()
TWILIO_PHONE    = os.getenv("TWILIO_PHONE_NUMBER", "").strip()
ALERT_PHONE     = os.getenv("ALERT_PHONE_NUMBER",  "").strip()
BASE44_ENTITY   = "Lead"

# Validate Groq model; fall back to a known-good default if unrecognised.
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
    log.warning("GROQ_MODEL '%s' not in known list — using default.", _GROQ_MODEL_INPUT)
    GROQ_MODEL = "llama-3.3-70b-versatile"
else:
    GROQ_MODEL = _GROQ_MODEL_INPUT or "llama-3.3-70b-versatile"

# Runtime flags
DRY_RUN     = os.getenv("DRY_RUN",     "false").strip().lower() == "true"
CITY_FILTER = os.getenv("CITY_FILTER", "").strip()

BASE44_URL  = f"https://api.base44.app/api/apps/{BASE44_APP_ID}/entities/{BASE44_ENTITY}"
B44_HEADERS = {"api_key": BASE44_API_KEY, "Content-Type": "application/json"}

# ══════════════════════════════════════════════════════════════
# COVERAGE  — 8 cities × 20 niches = 160 daily query combos
# ══════════════════════════════════════════════════════════════

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

# ══════════════════════════════════════════════════════════════
# REP CONFIGURATION
# ══════════════════════════════════════════════════════════════

SALES_REPS = [
    {
        "id":          "rep_1",
        "name":        "Alice",
        "phone":       ALERT_PHONE,
        "specialties": [
            "dental offices",
            "med spas and aesthetics",
            "physical therapy clinics",
            "chiropractors",
        ],
    },
    {
        "id":          "rep_2",
        "name":        "Bob",
        "phone":       ALERT_PHONE,
        "specialties": [
            "auto repair shops",
            "HVAC services",
            "plumbers",
            "roofing contractors",
            "electricians",
        ],
    },
    {
        "id":          "rep_3",
        "name":        "Carol",
        "phone":       ALERT_PHONE,
        "specialties": [
            "real estate agents",
            "insurance agents",
            "law firms",
            "accounting firms",
            "mortgage brokers",
        ],
    },
]

rep_load: dict[str, int] = {r["id"]: 0 for r in SALES_REPS}

# ══════════════════════════════════════════════════════════════
# TUNING CONSTANTS
# ══════════════════════════════════════════════════════════════

LEADS_PER_QUERY     = 5
MAX_RETRIES         = 3
RETRY_BACKOFF       = 2.0
API_SLEEP           = 0.5    # SerpAPI throttle
GROQ_SLEEP          = 0.3    # Groq throttle
B44_WRITE_SLEEP     = 0.4    # Base44 write throttle (avoid 429s)
WEBSITE_TIMEOUT     = 6
HIGH_PRIORITY_SMS   = 25     # Max individual outreach SMS per run
INTEL_TOP_N         = 10     # Leads fed to Agent 6 strategic brief
SMS_CHUNK_SIZE      = 1550   # Safe chunk size (Twilio hard limit 1600)
# GitHub Actions jobs time out at 30 min; leave 5 min for outreach agents.
MAX_RUNTIME_SECONDS = 1500

# ══════════════════════════════════════════════════════════════
# RUN METRICS
# ══════════════════════════════════════════════════════════════

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

# Lock guards list appends from multiple threads (ThreadPoolExecutor).
_metrics_lock = threading.Lock()


def _record_error(msg: str) -> None:
    with _metrics_lock:
        METRICS["errors"].append(msg)


# ══════════════════════════════════════════════════════════════
# MODULE-LEVEL SERVICE SINGLETONS
# Instantiated once at first use, not per-call.
# ══════════════════════════════════════════════════════════════

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


# ══════════════════════════════════════════════════════════════
# UTILITY FUNCTIONS
# ══════════════════════════════════════════════════════════════

def lead_fingerprint(name: str, address: str) -> str:
    raw = f"{name.strip().lower()}|{address.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def retry(fn, *args, retries: int = MAX_RETRIES, **kwargs):
    """Retry with exponential back-off; preserves the original traceback."""
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as exc:
            if attempt == retries - 1:
                raise exc
            wait = RETRY_BACKOFF * (2 ** attempt)
            log.warning("Retry %d/%d — %s — waiting %.1fs", attempt + 1, retries, exc, wait)
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
    log.info("  NOTE: Slack and CRM webhook disabled for this run")
    ok = True
    for k, v in required.items():
        if not v:
            log.error("  MISSING: %s", k)
            ok = False
        else:
            log.info("  OK:      %s = %s…", k, v[:6])
    if DRY_RUN:
        log.info("  MODE: DRY RUN — all writes suppressed")
    if CITY_FILTER:
        log.info("  FILTER: City restricted to '%s'", CITY_FILTER)
    return ok


# ══════════════════════════════════════════════════════════════
# GROQ AI LAYER
# ══════════════════════════════════════════════════════════════

def ask_groq(
    system: str,
    user: str,
    max_tokens: int = 900,
    temperature: float = 0.3,
) -> Optional[str]:
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
        log.error("Groq error: %s", exc)
        _record_error(f"Groq: {exc}")
        return None


def parse_json(raw: Optional[str]) -> Optional[dict]:
    """
    Safely parse a JSON string that may be wrapped in markdown fences.
    Strips ```json … ``` or ``` … ``` without corrupting JSON content.
    """
    if not raw:
        return None
    clean = raw.strip()
    if "```" in clean:
        parts = clean.split("```")
        for part in parts:
            # Strip an optional language marker (e.g. "json\n") on the first line only.
            candidate = re.sub(r"^[a-z]+\n", "", part.strip(), count=1)
            if candidate.startswith("{"):
                clean = candidate
                break
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        return None


# ══════════════════════════════════════════════════════════════
# BASE44 CRM LAYER
# ══════════════════════════════════════════════════════════════

def b44_list_existing() -> set:
    """
    Fetch all existing lead fingerprints for deduplication.
    Distinguishes a network error (warn + return empty) from a genuinely
    empty CRM (also returns empty — both are safe; worst case is a duplicate
    key error on create, not data loss).
    """
    if not BASE44_API_KEY or not BASE44_APP_ID:
        log.warning("Base44 credentials missing — skipping dedup prefetch")
        return set()
    try:
        r = requests.get(BASE44_URL, headers=B44_HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
        fps = {rec.get("fingerprint", "") for rec in data if rec.get("fingerprint")}
        log.info("  Dedup prefetch: %d records, %d fingerprints loaded", len(data), len(fps))
        return fps
    except requests.exceptions.RequestException as exc:
        log.warning("Dedup prefetch network error: %s — proceeding without dedup", exc)
        return set()
    except Exception as exc:
        log.warning("Dedup prefetch unexpected error: %s — proceeding without dedup", exc)
        return set()


def _b44_request(method: str, url: str, data: dict) -> Optional[dict]:
    """
    Shared Base44 HTTP helper with 429-aware exponential back-off.
    Adds B44_WRITE_SLEEP after every successful write to stay under rate limits.
    """
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.request(method, url, headers=B44_HEADERS, json=data, timeout=15)
            if r.status_code == 429:
                wait = B44_WRITE_SLEEP * (3 ** attempt)   # 0.4 → 1.2 → 3.6 s
                log.warning(
                    "Base44 rate-limited (429) — waiting %.1fs (attempt %d/%d)",
                    wait, attempt + 1, MAX_RETRIES,
                )
                time.sleep(wait)
                continue
            r.raise_for_status()
            time.sleep(B44_WRITE_SLEEP)   # polite pause after every success
            return r.json()
        except requests.exceptions.HTTPError as exc:
            log.error("Base44 HTTP error [%s %s]: %s", method, url, exc)
            _record_error(f"B44 {method}: {exc}")
            return None
        except Exception as exc:
            if attempt == MAX_RETRIES - 1:
                log.error("Base44 error [%s %s]: %s", method, url, exc)
                _record_error(f"B44 {method}: {exc}")
                return None
            wait = RETRY_BACKOFF * (2 ** attempt)
            log.warning("Base44 transient error — retrying in %.1fs: %s", wait, exc)
            time.sleep(wait)
    log.error("Base44 %s failed after %d attempts (429 rate limit)", method, MAX_RETRIES)
    _record_error(f"B44 {method}: max retries exceeded (429)")
    return None


def b44_create(data: dict) -> Optional[dict]:
    if DRY_RUN:
        log.info("    [DRY RUN] Skipping Base44 create")
        return {"_id": "dry_run_id"}
    return _b44_request("POST", BASE44_URL, _sanitize_b44(data))


def _sanitize_b44(data: dict) -> dict:
    """
    Scrub an update payload before sending to Base44.
    - Drops None values (Base44 rejects explicit nulls on PUT).
    - Converts lists → comma-separated strings.
    - Converts booleans to lowercase strings Base44 accepts.
    - Truncates strings longer than 2000 chars.
    - Drops any key whose value cannot be serialised as a scalar.
    """
    clean = {}
    for k, v in data.items():
        if v is None:
            continue
        if isinstance(v, list):
            v = ", ".join(str(i) for i in v)
        if isinstance(v, bool):
            v = str(v).lower()
        if isinstance(v, (int, float)):
            clean[k] = v
            continue
        if isinstance(v, str):
            clean[k] = v[:2000]
            continue
        # Skip anything else (dicts, sets, etc.)
    return clean


def b44_update(record_id: str, data: dict) -> Optional[dict]:
    """Skip the PUT for dry-run placeholders and actual dry-run mode."""
    if not record_id or record_id == "dry_run_id" or DRY_RUN:
        return {}
    return _b44_request("PUT", f"{BASE44_URL}/{record_id}", _sanitize_b44(data))


# ══════════════════════════════════════════════════════════════
# TWILIO SMS LAYER
# ══════════════════════════════════════════════════════════════

def _send_sms_chunk(to: str, body: str) -> bool:
    """Send a single pre-sized chunk. Caller ensures len(body) <= SMS_CHUNK_SIZE."""
    try:
        msg = _get_twilio().messages.create(body=body, from_=TWILIO_PHONE, to=to)
        log.info("  SMS sent → %s [%s]", to, msg.sid)
        with _metrics_lock:
            METRICS["sms_sent"] += 1
        return True
    except Exception as exc:
        err_str = str(exc)
        # Gracefully skip unverified-number errors on Twilio trial accounts.
        # These are expected until the account is upgraded — not a pipeline failure.
        if "21608" in err_str or "unverified" in err_str.lower():
            log.warning(
                "  SMS skipped (Twilio trial — unverified number %s). "
                "Upgrade Twilio account to enable SMS to any number.", to
            )
        else:
            log.error("Twilio error: %s", exc)
            _record_error(f"Twilio: {exc}")
        return False


def send_sms(to: str, body: str) -> bool:
    """
    Send an SMS, splitting into <= SMS_CHUNK_SIZE chunks at newline boundaries
    so Twilio's 1600-char hard limit never silently truncates content.

    NOTE: On Twilio trial accounts, SMS to unverified numbers is skipped
    gracefully with a warning rather than an error. Upgrade Twilio to enable
    full SMS outreach.
    """
    if DRY_RUN:
        log.info("  [DRY RUN] SMS → %s: %s…", to, body[:80])
        return True
    if not all([TWILIO_SID, TWILIO_TOKEN, TWILIO_PHONE, to]):
        log.warning("SMS skipped — missing Twilio config or recipient number")
        return False

    if len(body) <= SMS_CHUNK_SIZE:
        return _send_sms_chunk(to, body)

    # Split at newline boundaries where possible to preserve readability.
    chunks: list[str] = []
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
        if not _send_sms_chunk(to, chunk):
            success = False
        time.sleep(0.3)
    return success


# ══════════════════════════════════════════════════════════════
# SLACK / CRM STUBS  (disabled — no webhooks configured)
# ══════════════════════════════════════════════════════════════

def send_slack(payload: dict) -> bool:
    log.info("  [Slack disabled] Skipping Slack notification")
    return False


def push_crm(lead: dict) -> bool:
    return False


# ══════════════════════════════════════════════════════════════
# AGENT 1 — APEX LEAD CAPTURE
# ══════════════════════════════════════════════════════════════

def _serpapi_maps(query: str) -> list[dict]:
    try:
        results = GoogleSearch({
            "engine":  "google_maps",
            "q":       query,
            "api_key": SERPAPI_API_KEY,
            "type":    "search",
        }).get_dict()
        leads = []
        for item in results.get("local_results", [])[:LEADS_PER_QUERY]:
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
        log.warning("Maps search failed for '%s': %s", query, exc)
        _record_error(f"Maps '{query}': {exc}")
        return []


# Aggregator domains to exclude from organic results.
_ORGANIC_SKIP = ("yelp.com", "yellowpages", "bbb.org", "manta.com")


def _serpapi_organic(query: str) -> list[dict]:
    try:
        results = GoogleSearch({
            "engine":  "google",
            "q":       query,
            "api_key": SERPAPI_API_KEY,
            "num":     5,
        }).get_dict()
        leads = []
        for item in results.get("organic_results", []):
            link = item.get("link", "")
            if not link or any(d in link for d in _ORGANIC_SKIP):
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
                "snippet":   item.get("snippet", ""),
                "source":    "google_organic",
            })
        return leads
    except Exception as exc:
        log.warning("Organic search failed for '%s': %s", query, exc)
        _record_error(f"Organic '{query}': {exc}")
        return []


def agent_capture_leads(existing_fps: set) -> list[dict]:
    log.info("\n" + "=" * 62)
    log.info("AGENT 1 — APEX LEAD CAPTURE (Maps + Organic)")
    log.info("=" * 62)

    # Exact city match (normalised lowercase) to avoid substring false-positives.
    if CITY_FILTER:
        city_filter_norm = CITY_FILTER.strip().lower()
        cities = [c for c in CITIES if c.lower() == city_filter_norm]
        if not cities:
            log.warning(
                "CITY_FILTER '%s' matched no cities — running all cities", CITY_FILTER
            )
            cities = CITIES
    else:
        cities = CITIES

    METRICS["cities_queried"] = len(cities)
    METRICS["niches_queried"] = len(BUSINESS_TYPES)

    captured: list[dict] = []
    dupes         = 0
    total_queries = 0
    agent1_start  = time.time()

    for city in cities:
        for btype in BUSINESS_TYPES:
            # Check timeout at the top of each query iteration so we can exit
            # cleanly and still hand off to the outreach agents.
            if time.time() - agent1_start > MAX_RUNTIME_SECONDS:
                log.warning(
                    "  MAX RUNTIME reached — stopping Agent 1 early "
                    "to allow outreach to run"
                )
                break

            query = f"{btype} {city}"
            log.info("  → %s", query)
            total_queries += 1

            maps_leads = _serpapi_maps(query)
            time.sleep(API_SLEEP)
            organic_leads = _serpapi_organic(query)
            time.sleep(API_SLEEP)

            new_this_query = 0
            for raw in maps_leads + organic_leads:
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
                    # Mutate the set AFTER capturing the new-count increment.
                    existing_fps.add(fp)
                    captured.append(lead)
                    new_this_query += 1
                    METRICS["top_niches"][btype] = METRICS["top_niches"].get(btype, 0) + 1
                    METRICS["top_cities"][city]  = METRICS["top_cities"].get(city,  0) + 1

            log.info(
                "     Maps: %d | Organic: %d | New this query: %d",
                len(maps_leads), len(organic_leads), new_this_query,
            )
        else:
            # Inner loop completed without a break — continue outer loop.
            continue
        # Inner loop broke early (timeout) — break outer loop too.
        break

    METRICS["total_queries"] = total_queries
    METRICS["captured"]      = len(captured)
    METRICS["dupes_skipped"] = dupes

    log.info("\nAgent 1 Complete — Captured: %d | Dupes skipped: %d", len(captured), dupes)
    return captured


# ══════════════════════════════════════════════════════════════
# AGENT 2 — INTELLIGENCE RECON
# ══════════════════════════════════════════════════════════════

_PROBE_HEADERS = {"User-Agent": "Mozilla/5.0 LeadFlow-Probe/4.0"}


def _probe_website(url: str) -> dict:
    """
    Probe a website for liveness, SSL, speed, and redirect behaviour.
    User-Agent is set on all requests to avoid 403s from UA-sensitive servers.
    """
    null_result = {"live": False, "has_ssl": False, "slow": False, "redirect": False, "status": 0}
    if not url:
        return null_result
    if not url.startswith("http"):
        url = "https://" + url
    try:
        start = time.time()
        r = requests.get(
            url,
            timeout=WEBSITE_TIMEOUT,
            allow_redirects=True,
            headers=_PROBE_HEADERS,
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
        return {**null_result, "slow": True}


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
        for item in results.get("organic_results", []):
            link = item.get("link", "").lower()
            if "facebook.com"  in link: signals["facebook"]  = True
            if "instagram.com" in link: signals["instagram"] = True
            if "yelp.com"      in link: signals["yelp"]      = True
    except Exception:
        pass
    return signals


def _competitor_intel(name: str, btype: str, city: str) -> dict:
    bname_slug = name.lower().replace(" ", "").replace("'", "")
    intel      = {"top_competitor": "", "lead_owns_top_result": False}
    try:
        results = GoogleSearch({
            "engine":  "google",
            "q":       f"{btype} {city}",
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
    log.info("\n" + "=" * 62)
    log.info("AGENT 2 — INTELLIGENCE RECON")
    log.info("=" * 62)

    for lead in leads:
        name    = lead.get("name", "?")
        website = lead.get("website", "")
        city    = lead.get("city", "")
        btype   = lead.get("business_type", "")

        log.info("  Recon: %s", name)

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

        pain_points: list[str] = []
        if lead["no_website"]:
            pain_points.append("no website")
        elif not lead["website_live"]:
            pain_points.append("dead website")
        if not lead["website_has_ssl"]:
            pain_points.append("no SSL certificate")
        if lead["website_slow"]:
            pain_points.append("slow-loading website")
        if not lead["owns_search_result"]:
            pain_points.append("competitor outranks them")
        if (lead.get("reviews") or 0) < 10:
            pain_points.append("few online reviews")
        if not socials["facebook"]:
            pain_points.append("no Facebook presence")
        if not socials["instagram"]:
            pain_points.append("no Instagram presence")
        if not socials["yelp"]:
            pain_points.append("not on Yelp")

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
    log.info("\nAgent 2 Complete — Enriched: %d leads", len(leads))
    return leads


# ══════════════════════════════════════════════════════════════
# AGENT 3 — AI BRAIN: MULTI-PASS GROQ SCORING
# ══════════════════════════════════════════════════════════════

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

# Default values ensure all required keys exist even when Groq fails,
# preventing KeyErrors in downstream agents.
_LEAD_COPY_DEFAULTS: dict = {
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
    prompt = (
        f"Name:                {lead.get('name', 'Unknown')}\n"
        f"Business Type:       {lead.get('business_type', '')}\n"
        f"City:                {lead.get('city', '')}\n"
        f"Phone:               {lead.get('phone', 'MISSING')}\n"
        f"Rating:              {lead.get('rating', 0)} stars / {lead.get('reviews', 0)} reviews\n"
        f"Website Live:        {lead.get('website_live', False)}\n"
        f"Has SSL:             {lead.get('website_has_ssl', False)}\n"
        f"Website Slow:        {lead.get('website_slow', False)}\n"
        f"No Website:          {lead.get('no_website', False)}\n"
        f"Has Facebook:        {lead.get('has_facebook', False)}\n"
        f"Has Instagram:       {lead.get('has_instagram', False)}\n"
        f"Has Yelp:            {lead.get('has_yelp', False)}\n"
        f"Social Count:        {lead.get('social_presence_count', 0)} platforms\n"
        f"Top Competitor:      {lead.get('top_competitor', 'Unknown')}\n"
        f"Owns Search Result:  {lead.get('owns_search_result', False)}\n"
        f"Pain Points ({lead.get('pain_point_count', 0)}): {pains}\n"
    )
    return parse_json(ask_groq(SCORE_SYSTEM, prompt, max_tokens=400))


def _persona_lead(lead: dict) -> Optional[dict]:
    prompt = (
        f"Business: {lead.get('name')} | Type: {lead.get('business_type')} | City: {lead.get('city')}\n"
        f"Rating: {lead.get('rating', 0)} stars, {lead.get('reviews', 0)} reviews\n"
        f"Digital gaps: {', '.join(lead.get('pain_points', [])) or 'minimal'}\n"
    )
    return parse_json(ask_groq(PERSONA_SYSTEM, prompt, max_tokens=300))


def _copy_lead(lead: dict, score_data: dict, persona_data: dict) -> Optional[dict]:
    prompt = (
        f"Business: {lead.get('name')} | {lead.get('business_type')} | {lead.get('city')}\n"
        f"Score: {score_data.get('score', 5)}/10 | Priority: {score_data.get('priority', 'medium')}\n"
        f"Pain: {score_data.get('pain_summary', '')}\n"
        f"Decision-maker: {persona_data.get('decision_maker_title', 'Owner')}\n"
        f"Best hook: {persona_data.get('best_hook', '')}\n"
        f"Service rec: {score_data.get('service_recommendation', '')}\n"
    )
    return parse_json(ask_groq(COPY_SYSTEM, prompt, max_tokens=500))


def agent_qualify_leads(leads: list[dict]) -> list[dict]:
    log.info("\n" + "=" * 62)
    log.info("AGENT 3 — AI BRAIN: MULTI-PASS SCORING")
    log.info("=" * 62)

    qualified: list[dict] = []

    for lead in leads:
        name = lead.get("name", "?")
        log.info("  Scoring: %s", name)

        # ── Pass 1: Score ──
        score_data = _score_lead(lead)
        time.sleep(GROQ_SLEEP)

        if not score_data:
            # Groq failed — apply heuristic fallback.
            # Initialise ALL required fields to prevent KeyErrors downstream.
            has_phone  = bool(lead.get("phone"))
            pain_count = lead.get("pain_point_count", 0)
            lead.update({**_LEAD_COPY_DEFAULTS, "status": "Qualified", "priority": "medium", "ai_score": 5})
            if has_phone or pain_count >= 3:
                b44_update(lead.get("id", ""), {"status": "Qualified", "ai_score": 5})
                qualified.append(lead)
                log.info("  Qualified (heuristic fallback): %s", name)
            else:
                lead["status"] = "Unqualified"
                b44_update(lead.get("id", ""), {"status": "Unqualified"})
                log.info("  Unqualified (fallback): %s", name)
            continue

        if not score_data.get("qualified"):
            b44_update(lead.get("id", ""), {"status": "Unqualified"})
            log.info(
                "  Unqualified [%s/10]: %s — %s",
                score_data.get("score", 0), name, score_data.get("reason", ""),
            )
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
            **_LEAD_COPY_DEFAULTS,
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
            "  Qualified [%s | %d/10 | %s/mo]: %s",
            priority.upper(), score, lead.get("monthly_value_estimate", "?"), name,
        )

    METRICS["qualified"] = len(qualified)
    log.info("\nAgent 3 Complete — Qualified: %d leads", len(qualified))
    return qualified


# ══════════════════════════════════════════════════════════════
# AGENT 4 — STRATEGIC LEAD ASSIGNMENT
# ══════════════════════════════════════════════════════════════

def _best_rep(lead: dict) -> dict:
    """Return the best-matched rep with the lightest current load."""
    btype = lead.get("business_type", "")
    specialty_match = [
        r for r in SALES_REPS
        if any(spec in btype for spec in r.get("specialties", []))
    ]
    pool = specialty_match or SALES_REPS
    return min(pool, key=lambda r: rep_load[r["id"]])


def agent_assign_leads(leads: list[dict]) -> list[dict]:
    log.info("\n" + "=" * 62)
    log.info("AGENT 4 — STRATEGIC LEAD ASSIGNMENT")
    log.info("=" * 62)

    priority_order = {"high": 0, "medium": 1, "low": 2}
    leads_sorted = sorted(
        [l for l in leads if l.get("status") == "Qualified"],
        key=lambda x: (
            priority_order.get(x.get("priority", "low"), 2),
            -(x.get("ai_score") or 0),
        ),
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
            "  %s [%s | %d/10 | %s/mo] → %s",
            lead.get("name"),
            lead.get("priority", "?").upper(),
            lead.get("ai_score", 0),
            lead.get("monthly_value_estimate", "?"),
            rep["name"],
        )

    METRICS["assigned"] = len(assigned)
    log.info("\nAgent 4 Complete — Assigned: %d leads", len(assigned))
    log.info("Rep loads this run:")
    for r in SALES_REPS:
        log.info("  %s: %d leads", r["name"], rep_load[r["id"]])
    return assigned


# ══════════════════════════════════════════════════════════════
# AGENT 5 — OMNI OUTREACH
# ══════════════════════════════════════════════════════════════

def _parse_monthly_value(estimate: str) -> int:
    """
    Robustly extract a numeric value from a revenue estimate string.
    Handles formats like '$750-1200', '$1,200 - $1,800', '1200+', etc.
    Returns the midpoint of any range, or the single value found.
    """
    if not estimate:
        return 0
    nums = [int(n.replace(",", "")) for n in re.findall(r"\d[\d,]*", estimate)]
    if not nums:
        return 0
    return sum(nums) // len(nums)


def agent_launch_outreach(
    assigned: list[dict],
    captured: list[dict],
    qualified: list[dict],
) -> None:
    log.info("\n" + "=" * 62)
    log.info("AGENT 5 — OMNI OUTREACH")
    log.info("=" * 62)

    today = datetime.now().strftime("%m/%d")

    # ── Group leads by assigned rep ──
    rep_leads: dict[str, list] = {r["name"]: [] for r in SALES_REPS}
    for lead in assigned:
        rep_name = lead.get("assigned_to", "")
        if rep_name in rep_leads:
            rep_leads[rep_name].append(lead)

    # ── Rep digest SMS (one per rep, up to 8 leads summarised) ──
    for rep in SALES_REPS:
        rep_name  = rep["name"]
        to_phone  = rep.get("phone") or ALERT_PHONE
        rep_items = rep_leads.get(rep_name, [])
        if not rep_items:
            continue

        lines = [f"LeadFlow AI v4 | {rep_name}'s Leads — {today}\n"]
        for i, ld in enumerate(rep_items[:8], 1):
            lines.append(
                f"{i}. {ld.get('name')} ({ld.get('city', '')})\n"
                f"   Score: {ld.get('ai_score', 0)}/10 | {ld.get('priority', '?').upper()} | "
                f"Est: {ld.get('monthly_value_estimate', '?')}/mo\n"
                f"   Phone: {ld.get('phone', 'N/A')}\n"
                f"   Best time: {ld.get('best_contact_time', '?')}\n"
                f"   Hook: {ld.get('best_hook', '')[:80]}\n"
            )
        if len(rep_items) > 8:
            lines.append(f"...and {len(rep_items) - 8} more in your CRM.")

        send_sms(to_phone, "\n".join(lines))

    # ── Individual outreach SMS for high-priority leads ──
    highs = [l for l in assigned if l.get("priority") == "high" and l.get("outreach_sms")]
    log.info("  Sending %d individual outreach SMS", min(len(highs), HIGH_PRIORITY_SMS))

    for lead in highs[:HIGH_PRIORITY_SMS]:
        rep_phone = next(
            (r.get("phone") or ALERT_PHONE for r in SALES_REPS if r["name"] == lead.get("assigned_to")),
            ALERT_PHONE,
        )
        msg = (
            f"HIGH PRIORITY LEAD — {lead.get('name')}\n"
            f"Phone: {lead.get('phone', 'N/A')} | Score: {lead.get('ai_score', 0)}/10\n"
            f"City: {lead.get('city', '')} | Est: {lead.get('monthly_value_estimate', '?')}/mo\n"
            f"Best time to call: {lead.get('best_contact_time', '?')}\n\n"
            f"AI Outreach SMS:\n{lead.get('outreach_sms', '')}\n\n"
            f"Email A: {lead.get('email_subject_a', '')}\n"
            f"Email B: {lead.get('email_subject_b', '')}\n\n"
            f"Voicemail:\n{lead.get('voicemail_script', '')}"
        )
        send_sms(rep_phone, msg)  # chunking handled inside send_sms()
        time.sleep(0.5)

    # ── CRM webhook push ──
    for lead in assigned:
        push_crm(lead)
        time.sleep(0.2)

    # ── Slack rich digest ──
    top5 = sorted(assigned, key=lambda x: -(x.get("ai_score") or 0))[:5]
    top_lines = "\n".join(
        f"* {l.get('name')} ({l.get('city')}) — "
        f"{l.get('ai_score', 0)}/10 | {l.get('priority', '?').upper()} | "
        f"{l.get('monthly_value_estimate', '?')}/mo | -> {l.get('assigned_to', '?')}"
        for l in top5
    )
    total_monthly = sum(
        _parse_monthly_value(l.get("monthly_value_estimate", "")) for l in assigned
    )

    slack_payload = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"LeadFlow AI v4 — Daily Report {datetime.now().strftime('%m/%d/%Y')}",
                },
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
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Top 5 Leads:*\n{top_lines}"},
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": "Rep Loads:\n" + "\n".join(
                            f"{r['name']}: {rep_load[r['id']]} leads" for r in SALES_REPS
                        ),
                    },
                    {
                        "type": "mrkdwn",
                        "text": "Hot Niches:\n" + "\n".join(
                            f"{niche}: {count}"
                            for niche, count in sorted(
                                METRICS["top_niches"].items(), key=lambda x: -x[1]
                            )[:5]
                        ),
                    },
                ],
            },
        ]
    }
    send_slack(slack_payload)
    log.info("\nAgent 5 Complete — SMS: %d | CRM: %d", METRICS["sms_sent"], METRICS["crm_pushed"])


# ══════════════════════════════════════════════════════════════
# AGENT 6 — STRATEGIC INTELLIGENCE LOOP
# ══════════════════════════════════════════════════════════════

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
    log.info("\n" + "=" * 62)
    log.info("AGENT 6 — STRATEGIC INTELLIGENCE LOOP")
    log.info("=" * 62)

    if not assigned:
        log.info("  No assigned leads — skipping intelligence loop")
        return

    top_leads = sorted(assigned, key=lambda x: -(x.get("ai_score") or 0))[:INTEL_TOP_N]
    log.info("  Analysing top %d leads for strategic brief", len(top_leads))

    lead_lines = "\n".join(
        f"- {l.get('name')} | {l.get('business_type')} | {l.get('city')} | "
        f"Score: {l.get('ai_score', 0)}/10 | Pain: {l.get('pain_summary', '')} | "
        f"Est: {l.get('monthly_value_estimate', '?')}/mo"
        for l in top_leads
    )
    summary = (
        f"Run date:        {datetime.now().strftime('%Y-%m-%d')}\n"
        f"Total captured:  {METRICS['captured']}\n"
        f"Total qualified: {METRICS['qualified']}\n"
        f"Total assigned:  {METRICS['assigned']}\n\n"
        f"Top {len(top_leads)} leads this run:\n{lead_lines}\n\n"
        f"Hot niches (by lead volume): {json.dumps(METRICS['top_niches'])}\n"
        f"Hot cities  (by lead volume): {json.dumps(METRICS['top_cities'])}\n"
    )

    result = parse_json(ask_groq(INTEL_SYSTEM, summary, max_tokens=600, temperature=0.4))

    if not result:
        log.warning("  Intelligence loop produced no output")
        return

    log.info("  Strategic Brief Generated:")
    log.info("    Top Niche:   %s", result.get("top_opportunity_niche", ""))
    log.info("    Top City:    %s", result.get("top_opportunity_city", ""))
    log.info("    Insight:     %s", result.get("pattern_insight", ""))
    log.info("    Revenue Opp: %s", result.get("revenue_opportunity", ""))

    action_lines = "\n".join(f"* {a}" for a in result.get("action_items", []))

    slack_intel = {
        "blocks": [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"Strategic Intel Brief — {datetime.now().strftime('%m/%d/%Y')}",
                },
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*Top Niche to Target:*\n{result.get('top_opportunity_niche', '')}",
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Top City to Target:*\n{result.get('top_opportunity_city', '')}",
                    },
                ],
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Pattern Insight:*\n{result.get('pattern_insight', '')}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Revenue Opportunity:*\n{result.get('revenue_opportunity', '')}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Recommended Pitch Angle:*\n{result.get('recommended_pitch_angle', '')}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Action Items for Tomorrow:*\n{action_lines}",
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Next Run Focus:*\n{result.get('next_run_focus', '')}",
                },
            },
        ]
    }
    send_slack(slack_intel)
    log.info("  Intelligence brief sent to Slack")


# ══════════════════════════════════════════════════════════════
# MAIN PIPELINE  (per-agent fault isolation)
# ══════════════════════════════════════════════════════════════

def run_pipeline() -> None:
    start_time = datetime.now()
    METRICS["run_date"] = start_time.strftime("%Y-%m-%d %H:%M:%S CST")

    log.info("\n" + "=" * 62)
    log.info("LeadFlow AI v4.0 APEX — %s", METRICS["run_date"])
    if DRY_RUN:
        log.info("MODE: DRY RUN — no writes will occur")
    log.info("=" * 62)

    # ── Preflight checks ──
    if not check_credentials():
        log.error("Missing required credentials — aborting.")
        sys.exit(1)

    log.info("\n=== GROQ CONNECTIVITY TEST ===")
    test = ask_groq(
        "You are a helpful assistant.",
        "Reply with exactly one word: READY",
        max_tokens=5,
    )
    if not test:
        log.error("Groq connection failed — aborting.")
        sys.exit(1)
    log.info("Groq: %s", test)

    log.info("\n=== DEDUPLICATION PREFETCH ===")
    existing_fps = b44_list_existing()
    log.info("Found %d existing fingerprints in CRM", len(existing_fps))

    # ── Agent 1: Capture ──
    captured: list[dict] = []
    try:
        captured = agent_capture_leads(existing_fps)
    except Exception as exc:
        log.error("Agent 1 FAILED: %s", exc)
        _record_error(f"Agent1 fatal: {exc}")

    time.sleep(2)

    # ── Agent 2: Enrich ──
    enriched: list[dict] = captured
    if captured:
        try:
            enriched = agent_enrich_leads(captured)
        except Exception as exc:
            log.error("Agent 2 FAILED: %s — using unenriched leads", exc)
            _record_error(f"Agent2 fatal: {exc}")

    time.sleep(1)

    # ── Agent 3: Qualify ──
    qualified: list[dict] = []
    if enriched:
        try:
            qualified = agent_qualify_leads(enriched)
        except Exception as exc:
            log.error("Agent 3 FAILED: %s", exc)
            _record_error(f"Agent3 fatal: {exc}")

    time.sleep(1)

    # ── Agent 4: Assign ──
    assigned: list[dict] = []
    if qualified:
        try:
            assigned = agent_assign_leads(qualified)
        except Exception as exc:
            log.error("Agent 4 FAILED: %s", exc)
            _record_error(f"Agent4 fatal: {exc}")

    time.sleep(1)

    # ── Agent 5: Outreach ──
    if assigned:
        try:
            agent_launch_outreach(assigned, captured, qualified)
        except Exception as exc:
            log.error("Agent 5 FAILED: %s", exc)
            _record_error(f"Agent5 fatal: {exc}")

    time.sleep(1)

    # ── Agent 6: Intelligence loop ──
    try:
        agent_intelligence_loop(assigned)
    except Exception as exc:
        log.error("Agent 6 FAILED: %s", exc)
        _record_error(f"Agent6 fatal: {exc}")

    # ── Final metrics (written AFTER all outreach so SMS count is accurate) ──
    duration = int((datetime.now() - start_time).total_seconds())
    METRICS["duration_seconds"] = duration

    log.info("\n" + "=" * 62)
    log.info("LeadFlow AI v4.0 APEX — Run Complete")
    log.info("=" * 62)
    log.info("Duration:    %ds",   duration)
    log.info("Cities:      %d",    METRICS["cities_queried"])
    log.info("Niches:      %d",    METRICS["niches_queried"])
    log.info("Queries:     %d",    METRICS["total_queries"])
    log.info("Captured:    %d",    METRICS["captured"])
    log.info("Enriched:    %d",    METRICS["enriched"])
    log.info("Qualified:   %d",    METRICS["qualified"])
    log.info("Assigned:    %d",    METRICS["assigned"])
    log.info("SMS Sent:    %d",    METRICS["sms_sent"])
    log.info("CRM Synced:  %d",    METRICS["crm_pushed"])
    log.info("Errors:      %d",    len(METRICS["errors"]))
    log.info("=" * 62)

    metrics_path = f"leadflow_metrics_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(metrics_path, "w") as f:
        json.dump(METRICS, f, indent=2)
    log.info("Metrics written: %s", metrics_path)

    # Final operator summary SMS
    send_sms(
        ALERT_PHONE,
        f"LeadFlow AI v4 — Run Complete\n"
        f"Date: {datetime.now().strftime('%m/%d %I:%M %p')}\n"
        f"Captured: {METRICS['captured']} | Qualified: {METRICS['qualified']}\n"
        f"Assigned: {METRICS['assigned']} | SMS: {METRICS['sms_sent']}\n"
        f"CRM: {METRICS['crm_pushed']} | Errors: {len(METRICS['errors'])}\n"
        f"Duration: {duration}s",
    )


if __name__ == "__main__":
    run_pipeline()
