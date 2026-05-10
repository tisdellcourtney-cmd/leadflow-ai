"""
LeadFlow AI - Production Agent v2.0
Captures, qualifies, and assigns real business leads across Alabama.
Runs daily via GitHub Actions at 9:00 AM CST.
"""

import os
import json
import time
import requests
from datetime import datetime, timezone
from groq import Groq
from serpapi import GoogleSearch
from twilio.rest import Client as TwilioClient

# ── Credentials ──────────────────────────────────────────────────────────────
GROQ_API_KEY    = os.getenv("GROQ_API_KEY", "").strip()
SERPAPI_API_KEY = os.getenv("SERPAPI_API_KEY", "").strip()
BASE44_API_KEY  = os.getenv("BASE44_API_KEY", "").strip()
BASE44_APP_ID   = os.getenv("BASE44_APP_ID", "69f5719e46c0732dec4f4b06").strip()
TWILIO_SID      = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_TOKEN    = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_PHONE    = os.getenv("TWILIO_PHONE_NUMBER", "").strip()
GROQ_MODEL      = os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip()
BASE44_ENTITY   = os.getenv("BASE44_ENTITY", "Lead").strip()

BASE44_URL = f"https://api.base44.com/v1/apps/{BASE44_APP_ID}/entities/{BASE44_ENTITY}"
HEADERS    = {
    "ApiKey": BASE44_API_KEY,
    "Content-Type": "application/json"
}

# ── Target Markets ────────────────────────────────────────────────────────────
CITIES = [
    "Birmingham AL",
    "Huntsville AL",
    "Montgomery AL",
]

BUSINESS_TYPES = [
    "auto repair shops",
    "hair salons",
    "real estate agents",
    "insurance agents",
    "cleaning services",
    "restaurants",
    "dental offices",
    "HVAC services",
]

LEADS_PER_QUERY = 5

SALES_REPS = [
    {"id": "rep_1", "name": "Alice"},
    {"id": "rep_2", "name": "Bob"},
    {"id": "rep_3", "name": "Carol"},
]

# ── Startup Credential Check ──────────────────────────────────────────────────
def check_credentials():
    print("\n=== CREDENTIAL CHECK ===")
    required = {
        "GROQ_API_KEY":        GROQ_API_KEY,
        "SERPAPI_API_KEY":     SERPAPI_API_KEY,
        "BASE44_API_KEY":      BASE44_API_KEY,
        "TWILIO_ACCOUNT_SID":  TWILIO_SID,
        "TWILIO_AUTH_TOKEN":   TWILIO_TOKEN,
        "TWILIO_PHONE_NUMBER": TWILIO_PHONE,
    }
    all_good = True
    for key, val in required.items():
        if not val:
            print(f"❌ Missing: {key}")
            all_good = False
        else:
            print(f"✅ {key}: {val[:6]}...")
    return all_good

# ── Groq AI ───────────────────────────────────────────────────────────────────
def ask_groq(system, user, max_tokens=800):
    try:
        client = Groq(api_key=GROQ_API_KEY)
        response = client.chat.completions.create(
            messages=[
                {"role": "system", "content": system},
                {"role": "user",   "content": user},
            ],
            model=GROQ_MODEL,
            max_tokens=max_tokens,
        )
        return response.choices[0].message.content.strip()
    except Exception as e:
        print(f"❌ Groq error: {e}")
        return None

# ── Base44 Operations ─────────────────────────────────────────────────────────
def b44_create(data):
    try:
        r = requests.post(BASE44_URL, headers=HEADERS, json=data, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Base44 create error: {e}")
        return None

def b44_list():
    try:
        r = requests.get(BASE44_URL, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Base44 list error: {e}")
        return []

def b44_update(record_id, data):
    try:
        r = requests.put(f"{BASE44_URL}/{record_id}", headers=HEADERS, json=data, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"❌ Base44 update error: {e}")
        return None

# ── Twilio SMS ────────────────────────────────────────────────────────────────
def send_sms(message):
    try:
        client = TwilioClient(TWILIO_SID, TWILIO_TOKEN)
        msg = client.messages.create(
            body=message,
            from_=TWILIO_PHONE,
            to=TWILIO_PHONE,
        )
        print(f"✅ SMS sent: {msg.sid}")
        return True
    except Exception as e:
        print(f"❌ Twilio error: {e}")
        return False

# ── AGENT 1: Lead Capture ─────────────────────────────────────────────────────
def agent_capture_leads():
    print("\n" + "="*55)
    print("🔍 AGENT 1: Lead Capture Starting...")
    print("="*55)

    captured = []

    for city in CITIES:
        for business_type in BUSINESS_TYPES:
            query = f"{business_type} {city}"
            print(f"\n  📍 Searching: {query}")
            try:
                search = GoogleSearch({
                    "engine": "google_maps",
                    "q": query,
                    "api_key": SERPAPI_API_KEY,
                    "type": "search",
                })
                results = search.get_dict()
                local_results = results.get("local_results", [])[:LEADS_PER_QUERY]

                for result in local_results:
                    name = result.get("title", "Unknown")
                    lead_data = {
                        "name":          name,
                        "phone":         result.get("phone", ""),
                        "address":       result.get("address", ""),
                        "website":       result.get("website", ""),
                        "city":          city,
                        "business_type": business_type,
                        "rating":        result.get("rating", 0),
                        "reviews":       result.get("reviews", 0),
                        "status":        "New",
                        "created_at":    datetime.now(timezone.utc).isoformat(),
                    }

                    saved = b44_create(lead_data)
                    if saved:
                        captured.append(saved)
                        print(f"    ✅ Captured: {name}")
                    time.sleep(0.5)

            except Exception as e:
                print(f"    ❌ Search error for '{query}': {e}")

    print(f"\n📊 Agent 1 Complete — Total Captured: {len(captured)}")
    return captured

# ── AGENT 2: Lead Qualification ───────────────────────────────────────────────
def agent_qualify_leads(leads):
    print("\n" + "="*55)
    print("🧠 AGENT 2: Lead Qualification Starting...")
    print("="*55)

    qualified = []

    for lead in leads:
        if not lead or lead.get("status") != "New":
            continue

        name          = lead.get("name", "Unknown")
        phone         = lead.get("phone", "")
        rating        = lead.get("rating", 0)
        reviews       = lead.get("reviews", 0)
        business_type = lead.get("business_type", "")
        city          = lead.get("city", "")

        system_prompt = """You are an expert B2B sales qualification AI for a marketing agency in Alabama.
Qualify leads and respond ONLY with a valid JSON object, no extra text:
{
  "qualified": true or false,
  "score": 1-10,
  "priority": "high", "medium", or "low",
  "reason": "one sentence reason",
  "outreach_message": "personalized 2-sentence outreach SMS for this specific business"
}
Qualify if: rating >= 3.5 OR has phone number OR has 5+ reviews. Unqualify if no contact info at all."""

        user_prompt = f"""Qualify this Alabama business lead:
Name: {name}
Business Type: {business_type}
City: {city}
Phone: {phone}
Rating: {rating}
Reviews: {reviews}"""

        response = ask_groq(system_prompt, user_prompt, max_tokens=300)

        if not response:
            print(f"  ⚠️  Skipped (no AI response): {name}")
            continue

        try:
            clean = response.strip()
            if "```" in clean:
                clean = clean.split("```")[1].replace("json", "").strip()
            result = json.loads(clean)

            if result.get("qualified"):
                lead["status"]           = "Qualified"
                lead["ai_score"]         = result.get("score", 0)
                lead["priority"]         = result.get("priority", "medium")
                lead["outreach_message"] = result.get("outreach_message", "")
                b44_update(lead["id"], {
                    "status":   "Qualified",
                    "ai_score": result.get("score", 0),
                    "priority": result.get("priority", "medium"),
                })
                qualified.append(lead)
                print(f"  ⭐ Qualified [{result.get('priority','?').upper()}]: {name} (Score: {result.get('score')}/10)")
            else:
                b44_update(lead["id"], {"status": "Unqualified"})
                print(f"  ❌ Unqualified: {name} — {result.get('reason','')}")

        except (json.JSONDecodeError, Exception):
            if phone or (rating and float(str(rating)) >= 3.5):
                lead["status"]    = "Qualified"
                lead["priority"]  = "medium"
                b44_update(lead["id"], {"status": "Qualified"})
                qualified.append(lead)
                print(f"  ⭐ Qualified (fallback): {name}")
            else:
                b44_update(lead["id"], {"status": "Unqualified"})
                print(f"  ❌ Unqualified (fallback): {name}")

        time.sleep(0.3)

    print(f"\n📊 Agent 2 Complete — Total Qualified: {len(qualified)}")
    return qualified

# ── AGENT 3: Lead Assignment ──────────────────────────────────────────────────
def agent_assign_leads(leads):
    print("\n" + "="*55)
    print("📋 AGENT 3: Lead Assignment Starting...")
    print("="*55)

    assigned = []
    priority_order = {"high": 0, "medium": 1, "low": 2}
    leads_sorted = sorted(leads, key=lambda x: priority_order.get(x.get("priority", "low"), 2))

    for i, lead in enumerate(leads_sorted):
        if not lead or lead.get("status") != "Qualified":
            continue

        rep  = SALES_REPS[i % len(SALES_REPS)]
        name = lead.get("name", "Unknown")

        lead["assigned_to"] = rep["name"]
        lead["status"]      = "Assigned"

        b44_update(lead["id"], {
            "assigned_to": rep["name"],
            "status":      "Assigned",
        })
        assigned.append(lead)
        print(f"  → Assigned: {name} [{lead.get('priority','?').upper()}] to {rep['name']}")

        outreach = lead.get("outreach_message", "")
        if outreach and TWILIO_SID and TWILIO_TOKEN:
            send_sms(
                f"LeadFlow AI | New Lead for {rep['name']}:\n"
                f"{name}\n"
                f"📞 {lead.get('phone','N/A')}\n"
                f"📍 {lead.get('city','')}\n\n"
                f"{outreach}"
            )

        time.sleep(0.3)

    print(f"\n📊 Agent 3 Complete — Total Assigned: {len(assigned)}")
    return assigned

# ── MAIN RUNNER ───────────────────────────────────────────────────────────────
def run_agents():
    start_time = datetime.now()
    print("\n" + "="*55)
    print(f"🚀 LeadFlow AI v2.0 — {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*55)

    if not check_credentials():
        print("\n❌ Missing credentials — aborting.")
        return

    print("\n=== GROQ CONNECTION TEST ===")
    test = ask_groq("You are a helpful assistant.", "Say OK.", max_tokens=5)
    if test:
        print(f"✅ Groq connected: {test}")
    else:
        print("❌ Groq connection failed — aborting.")
        return

    captured  = agent_capture_leads()
    time.sleep(2)
    qualified = agent_qualify_leads(captured)
    time.sleep(1)
    assigned  = agent_assign_leads(qualified)

    duration = (datetime.now() - start_time).seconds
    print("\n" + "="*55)
    print("🏁 LeadFlow AI — Run Complete")
    print("="*55)
    print(f"⏱  Duration:   {duration} seconds")
    print(f"📥 Captured:   {len(captured)} leads")
    print(f"⭐ Qualified:  {len(qualified)} leads")
    print(f"✅ Assigned:   {len(assigned)} leads")
    print(f"📅 Finished:   {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*55)

    send_sms(
        f"🚀 LeadFlow AI Daily Report\n"
        f"📥 Captured: {len(captured)}\n"
        f"⭐ Qualified: {len(qualified)}\n"
        f"✅ Assigned: {len(assigned)}\n"
        f"⏱ Duration: {duration}s\n"
        f"📅 {datetime.now().strftime('%m/%d %I:%M %p')}"
    )

if __name__ == "__main__":
    run_agents()
