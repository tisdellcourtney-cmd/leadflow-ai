"""
LeadFlow AI — Production Agent v3.0
State-of-the-art lead generation, qualification, enrichment & assignment.
Targets Alabama businesses. Runs daily via GitHub Actions at 9:00 AM CST.

Architecture:
  Agent 1 — Lead Capture       (SerpAPI Google Maps + deduplication)
  Agent 2 — Deep Enrichment    (website presence, social signals, competitive gap)
  Agent 3 — AI Qualification   (Groq LLaMA multi-factor scoring)
  Agent 4 — Smart Assignment   (priority-weighted round-robin, rep load balancing)
  Agent 5 — Outreach Launch    (Twilio SMS per rep + Slack digest + CRM webhook)
"""

import os
import json
import time
import hashlib
import logging
import requests
from datetime import datetime, timezone
from groq import Groq
from serpapi import GoogleSearch
from twilio.rest import Client as TwilioClient

# ── Logging Setup ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("leadflow_v3.log"),
        logging.StreamHandler()
    ]
)
log = logging.getLogger("LeadFlow")

# ── Credentials ───────────────────────────────────────────────────────────────
GROQ_API_KEY        = os.getenv("GROQ_API_KEY", "").strip()
SERPAPI_API_KEY     = os.getenv("SERPAPI_API_KEY", "").strip()
BASE44_API_KEY      = os.getenv("BASE44_API_KEY", "").strip()
BASE44_APP_ID       = os.getenv("BASE44_APP_ID", "").strip()
TWILIO_SID          = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN        = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_PHONE        = os.getenv("TWILIO_PHONE_NUMBER", "").strip()
ALERT_PHONE         = os.getenv("ALERT_PHONE_NUMBER", "").strip()
SLACK_WEBHOOK       = os.getenv("SLACK_WEBHOOK_URL", "").strip()
CRM_WEBHOOK         = os.getenv("CRM_WEBHOOK_URL", "").strip()
GROQ_MODEL          = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
BASE44_ENTITY       = os.getenv("BASE44_ENTITY", "Lead").strip()

BASE44_URL = f"https://api.base44.app/api/apps/{BASE44_APP_ID}/entities/{BASE44_ENTITY}"
B44_HEADERS = {
    "api_key": BASE44_API_KEY,
    "Content-Type": "application/json"
}

# ── Coverage Configuration ────────────────────────────────────────────────────
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
]

LEADS_PER_QUERY    = 5
MAX_RETRIES        = 3
RETRY_BACKOFF      = 2.0     # seconds between retries
API_SLEEP          = 0.4     # throttle between SerpAPI calls
GROQ_SLEEP         = 0.3     # throttle between Groq calls

SALES_REPS = [
    {"id": "rep_1", "name": "Alice",  "phone": ALERT_PHONE},
    {"id": "rep_2", "name": "Bob",    "phone": ALERT_PHONE},
    {"id": "rep_3", "name": "Carol",  "phone": ALERT_PHONE},
]

# ── Rep load tracker (in-memory for this run) ─────────────────────────────────
rep_load = {rep["id"]: 0 for rep in SALES_REPS}


# ── Utilities ─────────────────────────────────────────────────────────────────

def lead_fingerprint(name: str, address: str) -> str:
    """Stable hash used for deduplication."""
    raw = f"{name.strip().lower()}|{address.strip().lower()}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def retry(fn, *args, retries=MAX_RETRIES, **kwargs):
    """Simple exponential-backoff retry wrapper."""
    for attempt in range(retries):
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if attempt == retries - 1:
                raise
            wait = RETRY_BACKOFF * (2 ** attempt)
            log.warning(f"Retry {attempt+1}/{retries} after error: {e} — waiting {wait}s")
            time.sleep(wait)


# ── Credential Check ──────────────────────────────────────────────────────────

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
    all_good = True
    for key, val in required.items():
        if not val:
            log.error(f"  Missing: {key}")
            all_good = False
        else:
            log.info(f"  OK: {key} = {val[:6]}...")
    return all_good


# ── Groq AI ───────────────────────────────────────────────────────────────────

def ask_groq(system: str, user: str, max_tokens: int = 800) -> str | None:
    try:
        client = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            model=GROQ_MODEL,
            max_tokens=max_tokens,
            temperature=0.3,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        log.error(f"Groq error: {e}")
        return None


def parse_json_response(raw: str) -> dict | None:
    """Safely extract JSON from a Groq response, stripping markdown fences."""
    if not raw:
        return None
    clean = raw.strip()
    if "```" in clean:
        parts = clean.split("```")
        for part in parts:
            part = part.strip().lstrip("json").strip()
            if part.startswith("{"):
                clean = part
                break
    try:
        return json.loads(clean)
    except json.JSONDecodeError:
        return None


# ── Base44 CRM ────────────────────────────────────────────────────────────────

def b44_list_existing() -> set:
    """Return set of fingerprints of all existing leads (for deduplication)."""
    try:
        r = requests.get(BASE44_URL, headers=B44_HEADERS, timeout=20)
        r.raise_for_status()
        records = r.json()
        return {rec.get("fingerprint", "") for rec in records if rec.get("fingerprint")}
    except Exception as e:
        log.warning(f"Could not fetch existing leads for dedup: {e}")
        return set()


def b44_create(data: dict) -> dict | None:
    try:
        r = retry(requests.post, BASE44_URL, headers=B44_HEADERS, json=data, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Base44 create error: {e}")
        return None


def b44_update(record_id: str, data: dict) -> dict | None:
    try:
        r = retry(requests.put, f"{BASE44_URL}/{record_id}", headers=B44_HEADERS, json=data, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.error(f"Base44 update error [{record_id}]: {e}")
        return None


# ── Twilio SMS ────────────────────────────────────────────────────────────────

def send_sms(to_number: str, message: str) -> bool:
    if not (TWILIO_SID and TWILIO_TOKEN and TWILIO_PHONE and to_number):
        log.warning("SMS skipped — missing Twilio config")
        return False
    try:
        client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
        msg = client.messages.create(
            body=message[:1600],     # Twilio hard limit
            from_=TWILIO_PHONE,
            to=to_number,
        )
        log.info(f"  SMS sent to {to_number}: {msg.sid}")
        return True
    except Exception as e:
        log.error(f"Twilio error: {e}")
        return False


# ── Slack ─────────────────────────────────────────────────────────────────────

def send_slack(payload: dict) -> bool:
    if not SLACK_WEBHOOK:
        return False
    try:
        r = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        r.raise_for_status()
        return True
    except Exception as e:
        log.error(f"Slack error: {e}")
        return False


# ── CRM Webhook ───────────────────────────────────────────────────────────────

def push_to_crm(lead: dict) -> bool:
    if not CRM_WEBHOOK:
        return False
    try:
        r = requests.post(CRM_WEBHOOK, json=lead, timeout=10)
        r.raise_for_status()
        log.info(f"  CRM webhook fired for: {lead.get('name', '?')}")
        return True
    except Exception as e:
        log.error(f"CRM webhook error: {e}")
        return False


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 1 — Lead Capture
# ─────────────────────────────────────────────────────────────────────────────

def agent_capture_leads(existing_fingerprints: set) -> list[dict]:
    log.info("\n" + "=" * 60)
    log.info("AGENT 1: Lead Capture")
    log.info("=" * 60)

    captured = []
    skipped_dupes = 0

    for city in CITIES:
        for business_type in BUSINESS_TYPES:
            query = f"{business_type} {city}"
            log.info(f"  Searching: {query}")
            try:
                search = GoogleSearch({
                    "engine":  "google_maps",
                    "q":       query,
                    "api_key": SERPAPI_API_KEY,
                    "type":    "search",
                })
                results     = search.get_dict()
                local_items = results.get("local_results", [])[:LEADS_PER_QUERY]

                for item in local_items:
                    name    = item.get("title", "Unknown Business")
                    address = item.get("address", "")
                    fp      = lead_fingerprint(name, address)

                    if fp in existing_fingerprints:
                        skipped_dupes += 1
                        log.info(f"    Dupe skipped: {name}")
                        continue

                    lead_data = {
                        "fingerprint":   fp,
                        "name":          name,
                        "phone":         item.get("phone", ""),
                        "address":       address,
                        "website":       item.get("website", ""),
                        "city":          city,
                        "business_type": business_type,
                        "rating":        item.get("rating", 0),
                        "reviews":       item.get("reviews", 0),
                        "place_id":      item.get("place_id", ""),
                        "thumbnail":     item.get("thumbnail", ""),
                        "status":        "New",
                        "created_at":    datetime.now(timezone.utc).isoformat(),
                        "run_date":      datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    }

                    saved = b44_create(lead_data)
                    if saved:
                        lead_data["id"] = saved.get("_id") or saved.get("id", "")
                        captured.append(lead_data)
                        existing_fingerprints.add(fp)
                        log.info(f"    Captured: {name}")

                    time.sleep(API_SLEEP)

            except Exception as e:
                log.error(f"  Search error for '{query}': {e}")

    log.info(f"\nAgent 1 Complete — Captured: {len(captured)} | Dupes skipped: {skipped_dupes}")
    return captured


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 2 — Deep Enrichment
# ─────────────────────────────────────────────────────────────────────────────

def check_website_health(url: str) -> dict:
    """Quick probe of a website for live presence and basic signals."""
    if not url:
        return {"live": False, "has_ssl": False, "slow": False}
    try:
        if not url.startswith("http"):
            url = "https://" + url
        start = time.time()
        r = requests.get(url, timeout=6, allow_redirects=True,
                         headers={"User-Agent": "Mozilla/5.0 LeadFlow-Probe/3.0"})
        latency = time.time() - start
        return {
            "live":    r.status_code < 400,
            "has_ssl": url.startswith("https"),
            "slow":    latency > 3.5,
            "status":  r.status_code,
        }
    except Exception:
        return {"live": False, "has_ssl": False, "slow": True}


def agent_enrich_leads(leads: list[dict]) -> list[dict]:
    log.info("\n" + "=" * 60)
    log.info("AGENT 2: Deep Enrichment")
    log.info("=" * 60)

    for lead in leads:
        name    = lead.get("name", "?")
        website = lead.get("website", "")
        city    = lead.get("city", "")
        btype   = lead.get("business_type", "")

        log.info(f"  Enriching: {name}")

        # Website health probe
        site_signals = check_website_health(website)
        lead["website_live"]    = site_signals.get("live", False)
        lead["website_has_ssl"] = site_signals.get("has_ssl", False)
        lead["website_slow"]    = site_signals.get("slow", False)
        lead["no_website"]      = not bool(website)

        # Competitive gap check via SerpAPI text search
        try:
            comp_query = f'"{name}" {city} marketing OR SEO OR ads'
            comp_search = GoogleSearch({
                "engine":  "google",
                "q":       comp_query,
                "api_key": SERPAPI_API_KEY,
                "num":     3,
            })
            comp_results = comp_search.get_dict()
            organic = comp_results.get("organic_results", [])
            # Check if business controls its own top results
            business_slug = name.lower().replace(" ", "").replace("'", "")
            owns_top_result = any(
                business_slug in r.get("link", "").lower()
                for r in organic[:3]
            )
            lead["digital_presence_score"] = len(organic)
            lead["owns_search_result"]     = owns_top_result
        except Exception as e:
            log.warning(f"  Competitive check failed for {name}: {e}")
            lead["digital_presence_score"] = 0
            lead["owns_search_result"]     = False

        # Pain-point score: the more gaps, the hotter the opportunity
        pain_points = []
        if lead.get("no_website"):            pain_points.append("no website")
        if not lead.get("website_live"):      pain_points.append("dead/missing site")
        if not lead.get("website_has_ssl"):   pain_points.append("no SSL")
        if lead.get("website_slow"):          pain_points.append("slow website")
        if not lead.get("owns_search_result"):pain_points.append("low search visibility")
        if (lead.get("reviews") or 0) < 10:  pain_points.append("few reviews")

        lead["pain_points"]      = pain_points
        lead["pain_point_count"] = len(pain_points)

        b44_update(lead.get("id", ""), {
            "website_live":           lead["website_live"],
            "website_has_ssl":        lead["website_has_ssl"],
            "website_slow":           lead["website_slow"],
            "no_website":             lead["no_website"],
            "digital_presence_score": lead["digital_presence_score"],
            "pain_point_count":       lead["pain_point_count"],
            "pain_points":            ", ".join(pain_points),
        })

        time.sleep(API_SLEEP)

    log.info(f"\nAgent 2 Complete — Enriched: {len(leads)} leads")
    return leads


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 3 — AI Qualification & Scoring
# ─────────────────────────────────────────────────────────────────────────────

QUAL_SYSTEM = """You are a senior B2B sales strategist for a digital marketing agency in Alabama.
Your job is to score and qualify local business leads based on their digital presence gaps and contact data.

Respond ONLY with a valid JSON object — no preamble, no markdown:
{
  "qualified": true | false,
  "score": 1-10,
  "priority": "high" | "medium" | "low",
  "reason": "one sentence explanation of this score",
  "pain_summary": "one sentence describing their biggest marketing gap",
  "outreach_sms": "personalized 2-sentence SMS for this specific business owner. Mention their city and a specific pain point.",
  "outreach_email_subject": "compelling email subject line under 8 words",
  "service_recommendation": "top 1-2 services to pitch this business"
}

Scoring guide:
  10 — No website, no SSL, poor search visibility, has phone: perfect prospect
  8-9 — Dead or slow website, reachable, 3+ pain points
  6-7 — Has a basic site but poor SEO or few reviews
  4-5 — Decent site, minor gaps
  1-3 — Well-established digital presence, not a strong prospect

Qualify if score >= 5 OR has phone AND 2+ pain points.
Unqualify if score <= 3 AND no phone."""


def agent_qualify_leads(leads: list[dict]) -> list[dict]:
    log.info("\n" + "=" * 60)
    log.info("AGENT 3: AI Qualification & Scoring")
    log.info("=" * 60)

    qualified = []

    for lead in leads:
        if not lead or lead.get("status") != "New":
            continue

        name    = lead.get("name", "Unknown")
        phone   = lead.get("phone", "")
        rating  = lead.get("rating", 0)
        reviews = lead.get("reviews", 0)
        btype   = lead.get("business_type", "")
        city    = lead.get("city", "")
        pains   = ", ".join(lead.get("pain_points", [])) or "none detected"

        user_prompt = f"""Qualify this Alabama business lead:

Name:          {name}
Business Type: {btype}
City:          {city}
Phone:         {phone or "MISSING"}
Rating:        {rating} stars
Reviews:       {reviews}
Website Live:  {lead.get("website_live", False)}
Has SSL:       {lead.get("website_has_ssl", False)}
Website Slow:  {lead.get("website_slow", False)}
No Website:    {lead.get("no_website", False)}
Pain Points:   {pains}
Pain Count:    {lead.get("pain_point_count", 0)}"""

        raw = ask_groq(QUAL_SYSTEM, user_prompt, max_tokens=400)
        result = parse_json_response(raw)

        if result and result.get("qualified"):
            score    = result.get("score", 5)
            priority = result.get("priority", "medium")
            lead.update({
                "status":                   "Qualified",
                "ai_score":                 score,
                "priority":                 priority,
                "ai_reason":                result.get("reason", ""),
                "pain_summary":             result.get("pain_summary", ""),
                "outreach_sms":             result.get("outreach_sms", ""),
                "outreach_email_subject":   result.get("outreach_email_subject", ""),
                "service_recommendation":   result.get("service_recommendation", ""),
            })
            b44_update(lead.get("id", ""), {
                "status":                 "Qualified",
                "ai_score":               score,
                "priority":               priority,
                "ai_reason":              result.get("reason", ""),
                "pain_summary":           result.get("pain_summary", ""),
                "service_recommendation": result.get("service_recommendation", ""),
            })
            qualified.append(lead)
            log.info(f"  Qualified [{priority.upper()} | {score}/10]: {name}")

        elif result and not result.get("qualified"):
            b44_update(lead.get("id", ""), {"status": "Unqualified"})
            log.info(f"  Unqualified: {name} — {result.get('reason', '')}")

        else:
            # Fallback: qualify if has phone or high rating
            if phone or float(str(rating or 0)) >= 3.5:
                lead.update({
                    "status":   "Qualified",
                    "priority": "medium",
                    "ai_score": 5,
                })
                b44_update(lead.get("id", ""), {"status": "Qualified", "ai_score": 5})
                qualified.append(lead)
                log.info(f"  Qualified (fallback): {name}")
            else:
                b44_update(lead.get("id", ""), {"status": "Unqualified"})
                log.info(f"  Unqualified (fallback): {name}")

        time.sleep(GROQ_SLEEP)

    log.info(f"\nAgent 3 Complete — Qualified: {len(qualified)} leads")
    return qualified


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 4 — Smart Lead Assignment
# ─────────────────────────────────────────────────────────────────────────────

def agent_assign_leads(leads: list[dict]) -> list[dict]:
    log.info("\n" + "=" * 60)
    log.info("AGENT 4: Smart Lead Assignment")
    log.info("=" * 60)

    # Sort: high → medium → low, then by AI score desc
    priority_order = {"high": 0, "medium": 1, "low": 2}
    leads_sorted = sorted(
        leads,
        key=lambda x: (
            priority_order.get(x.get("priority", "low"), 2),
            -(x.get("ai_score") or 0),
        )
    )

    assigned = []

    for lead in leads_sorted:
        if lead.get("status") != "Qualified":
            continue

        # Load-balance: pick rep with fewest assignments this run
        rep = min(SALES_REPS, key=lambda r: rep_load[r["id"]])
        rep_load[rep["id"]] += 1

        lead["assigned_to"]    = rep["name"]
        lead["assigned_rep_id"]= rep["id"]
        lead["status"]         = "Assigned"
        lead["assigned_at"]    = datetime.now(timezone.utc).isoformat()

        b44_update(lead.get("id", ""), {
            "assigned_to":     rep["name"],
            "assigned_rep_id": rep["id"],
            "status":          "Assigned",
            "assigned_at":     lead["assigned_at"],
        })

        assigned.append(lead)
        log.info(f"  {lead.get('name')} [{lead.get('priority','?').upper()} | {lead.get('ai_score',0)}/10] → {rep['name']}")

    log.info(f"\nAgent 4 Complete — Assigned: {len(assigned)} leads")
    log.info("Rep load this run:")
    for rep in SALES_REPS:
        log.info(f"  {rep['name']}: {rep_load[rep['id']]} leads")
    return assigned


# ─────────────────────────────────────────────────────────────────────────────
# AGENT 5 — Outreach Launch
# ─────────────────────────────────────────────────────────────────────────────

def agent_launch_outreach(assigned: list[dict], captured: list[dict], qualified: list[dict]) -> None:
    log.info("\n" + "=" * 60)
    log.info("AGENT 5: Outreach Launch")
    log.info("=" * 60)

    # Group by rep
    rep_leads: dict[str, list] = {rep["name"]: [] for rep in SALES_REPS}
    for lead in assigned:
        rep_name = lead.get("assigned_to", "")
        if rep_name in rep_leads:
            rep_leads[rep_name].append(lead)

    # Per-rep SMS batch
    for rep in SALES_REPS:
        rep_name = rep["name"]
        to_phone = rep.get("phone") or ALERT_PHONE
        leads_for_rep = rep_leads.get(rep_name, [])

        if not leads_for_rep:
            continue

        # Build concise multi-lead SMS
        lines = [f"LeadFlow AI | {rep_name}'s leads for {datetime.now().strftime('%m/%d')}:\n"]
        for i, lead in enumerate(leads_for_rep[:8], 1):   # cap at 8 per SMS
            pain = lead.get("pain_summary", "") or ", ".join(lead.get("pain_points", [])[:2])
            lines.append(
                f"{i}. {lead.get('name')} ({lead.get('city','')})\n"
                f"   Score: {lead.get('ai_score',0)}/10 | {lead.get('priority','?').upper()}\n"
                f"   Phone: {lead.get('phone','N/A')}\n"
                f"   Hook: {pain[:80]}\n"
            )
        if len(leads_for_rep) > 8:
            lines.append(f"...and {len(leads_for_rep) - 8} more in your CRM.")

        send_sms(to_phone, "\n".join(lines))

    # Individual outreach SMS per HIGH-priority lead (using AI-generated message)
    high_priority = [l for l in assigned if l.get("priority") == "high" and l.get("outreach_sms")]
    log.info(f"  Sending {len(high_priority)} individual outreach SMS for high-priority leads")
    for lead in high_priority[:20]:   # cap to avoid excessive SMS cost
        rep_phone = next(
            (r.get("phone") or ALERT_PHONE for r in SALES_REPS if r["name"] == lead.get("assigned_to")),
            ALERT_PHONE
        )
        msg = (
            f"NEW HIGH PRIORITY LEAD — {lead.get('name')}\n"
            f"Phone: {lead.get('phone', 'N/A')}\n"
            f"City: {lead.get('city', '')}\n"
            f"Score: {lead.get('ai_score', 0)}/10\n\n"
            f"AI Outreach Draft:\n{lead.get('outreach_sms', '')}"
        )
        send_sms(rep_phone, msg)
        time.sleep(0.5)

    # Push each assigned lead to external CRM webhook
    crm_pushed = 0
    for lead in assigned:
        if push_to_crm(lead):
            crm_pushed += 1
        time.sleep(0.2)

    # Slack daily digest
    top_leads = sorted(assigned, key=lambda x: -(x.get("ai_score") or 0))[:5]
    top_lines = "\n".join(
        f"• *{l.get('name')}* ({l.get('city')}) — Score {l.get('ai_score',0)}/10 | {l.get('priority','?').upper()} | → {l.get('assigned_to','?')}"
        for l in top_leads
    )

    slack_payload = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"LeadFlow AI v3 — Daily Report {datetime.now().strftime('%m/%d/%Y')}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Captured:* {len(captured)}"},
                    {"type": "mrkdwn", "text": f"*Qualified:* {len(qualified)}"},
                    {"type": "mrkdwn", "text": f"*Assigned:* {len(assigned)}"},
                    {"type": "mrkdwn", "text": f"*CRM Synced:* {crm_pushed}"},
                ]
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Top 5 Leads Today:*\n{top_lines}"}
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*Rep Loads:*\n" + "\n".join(
                        f"{r['name']}: {rep_load[r['id']]}" for r in SALES_REPS
                    )},
                ]
            }
        ]
    }
    if send_slack(slack_payload):
        log.info("  Slack digest sent")

    log.info(f"\nAgent 5 Complete — SMS batches sent | CRM synced: {crm_pushed}")


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline():
    start_time = datetime.now()
    log.info("\n" + "=" * 60)
    log.info(f"LeadFlow AI v3.0 — {start_time.strftime('%Y-%m-%d %H:%M:%S')} CST")
    log.info("=" * 60)

    if not check_credentials():
        log.error("Missing credentials — aborting.")
        return

    # Groq connectivity test
    log.info("\n=== GROQ CONNECTION TEST ===")
    test = ask_groq("You are a helpful assistant.", "Say READY in one word.", max_tokens=5)
    if test:
        log.info(f"Groq connected: {test}")
    else:
        log.error("Groq connection failed — aborting.")
        return

    # Fetch existing fingerprints to avoid processing dupes
    log.info("\n=== DEDUPLICATION PREFETCH ===")
    existing_fps = b44_list_existing()
    log.info(f"Found {len(existing_fps)} existing lead fingerprints in CRM")

    # Run pipeline
    captured  = agent_capture_leads(existing_fps)
    time.sleep(2)
    enriched  = agent_enrich_leads(captured)
    time.sleep(1)
    qualified = agent_qualify_leads(enriched)
    time.sleep(1)
    assigned  = agent_assign_leads(qualified)
    agent_launch_outreach(assigned, captured, qualified)

    # Final summary
    duration = (datetime.now() - start_time).seconds
    log.info("\n" + "=" * 60)
    log.info("LeadFlow AI v3.0 — Run Complete")
    log.info("=" * 60)
    log.info(f"Duration:    {duration}s")
    log.info(f"Cities:      {len(CITIES)}")
    log.info(f"Query types: {len(BUSINESS_TYPES)}")
    log.info(f"Captured:    {len(captured)}")
    log.info(f"Enriched:    {len(enriched)}")
    log.info(f"Qualified:   {len(qualified)}")
    log.info(f"Assigned:    {len(assigned)}")
    log.info("=" * 60)

    # Final alert SMS
    send_sms(ALERT_PHONE,
        f"LeadFlow AI v3 Daily Summary\n"
        f"Date: {datetime.now().strftime('%m/%d %I:%M %p')}\n"
        f"Captured:  {len(captured)}\n"
        f"Qualified: {len(qualified)}\n"
        f"Assigned:  {len(assigned)}\n"
        f"Duration:  {duration}s"
    )


if __name__ == "__main__":
    run_pipeline()
