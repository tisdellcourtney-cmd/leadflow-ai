import asyncio
import os
import json
import requests
from datetime import datetime, timedelta, UTC # Import UTC
import time # Added import for time module
import nest_asyncio # Re-added nest_asyncio
from groq import Groq # Import Groq client

# ANTHROPIC_API_KEY is no longer used, switching to GROQ_API_KEY
GROQ_API_KEY      = os.getenv("GROQ_API_KEY")
BASE44_APP_ID     = os.getenv("BASE44_APP_ID").strip() # Added .strip()
BASE44_API_KEY    = os.getenv("BASE44_API_KEY").strip() # Added .strip()
BASE44_API_URL    = f"https://api.base44.app/api/apps/{BASE44_APP_ID}/entities"

HEADERS = {
    "Content-Type": "application/json",
    "api_key": BASE44_API_KEY
}

def b44_list(entity, filters=None):
    url = f"{BASE44_API_URL}/{entity}"
    params = {}
    if filters:
        params["filters"] = json.dumps(filters)
    print(f"[DEBUG] b44_list URL: {url}, Params: {params}") # Debug print
    r = requests.get(url, headers=HEADERS, params=params)
    r.raise_for_status()
    return r.json()

def b44_create(entity, data):
    url = f"{BASE44_API_URL}/{entity}"
    print(f"[DEBUG] Sending to b44_create: Entity={entity}, Data={json.dumps(data)}") # Debug print
    r = requests.post(url, headers=HEADERS, json=data)
    r.raise_for_status()
    response_data = r.json()
    print(f"[DEBUG] Received from b44_create: {json.dumps(response_data)}") # Debug print
    return response_data

def b44_update(entity, record_id, data):
    url = f"{BASE44_API_URL}/{entity}/{record_id}"
    r = requests.put(url, headers=HEADERS, json=data)
    r.raise_for_status()
    return r.json()

def ask_groq(system, user, model="llama-3.3-70b-versatile", max_tokens=800):
    client = Groq(api_key=GROQ_API_KEY)
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": system,
            },
            {
                "role": "user",
                "content": user,
            }
        ],
        model=model,
        max_tokens=max_tokens,
    )
    response_content = chat_completion.choices[0].message.content
    return response_content


    client = Groq(api_key=GROQ_API_KEY)
    chat_completion = client.chat.completions.create(
        messages=[
            {
                "role": "system",
                "content": system,
            },
            {
                "role": "user",
                "content": user,
            }
        ],
        model=model,
        temperature=0.0,
        max_tokens=max_tokens,
    )
    response_content = chat_completion.choices[0].message.content
    print(f"[DEBUG_GROQ] Groq response: {response_content}")
    return response_content

def agent_capture_leads():
    print("\n\U0001f50d [AGENT 1] Lead Capture Starting...")
from serpapi import GoogleSearch
    
    raw_leads = []
    
    search_queries = [
        "auto repair shops Birmingham AL",
        "hair salons Birmingham AL", 
        "real estate agents Birmingham AL",
        "insurance agents Birmingham AL",
        "cleaning services Birmingham AL",
    ]
    
    for query in search_queries:
        params = {
            "engine": "google_maps",
            "q": query,
            "api_key": SERPAPI_API_KEY,
            "type": "search"
        }
        
        search = GoogleSearch(params)
        results = search.get_dict()
        
        for result in results.get("local_results", [])[:2]:
            raw_leads.append({
                "name": result.get("title", "Unknown"),
                "phone": result.get("phone", "N/A"),
                "email": f"info@{result.get('title','lead').lower().replace(' ','')}. com",
                "address": result.get("address", "Birmingham, AL"),
                "website": result.get("website", "N/A"),
                "rating": result.get("rating", "N/A"),
            })
    captured = []
    for lead in raw_leads:
        lead["status"]     = "New"
        lead["created_at"] = datetime.now(UTC).isoformat()
        captured.append(b44_create("Lead", lead))
        print(f"\u2705 Captured Lead: {lead['name']}")
        time.sleep(1)
    print("\u2705 [AGENT 1] Lead Capture Complete.")
    return captured

def agent_qualify_leads(leads_to_qualify):
    print("\n\U0001f9d0 [AGENT 2] Lead Qualification Starting...")

    new_leads = []
    for lead in leads_to_qualify:
        current_status = lead.get("status")
        current_business_type = lead.get("business_type")
        current_city = lead.get("city")

        # Filter for 'New' leads that have business_type and city fields, as required for qualification
        if current_status == "New" and current_business_type and current_city:
            new_leads.append(lead)
        else:
            print(f"[WARN] Lead {lead.get('name', 'N/A')} skipped for qualification due to missing 'New' status, business_type, or city.")

    print(f"[DEBUG] Leads identified for qualification: {len(new_leads)}")

    qualified_leads = []
    if not new_leads:
        print("No new leads to qualify.")
        return qualified_leads

    for lead in new_leads:
        # These fields are guaranteed to exist due to the filtering above
        business_type = lead['business_type']
        city = lead['city']

        prompt = f"""
        You are a lead qualification agent for a marketing agency.
        Your task is to qualify leads based on their business type and location.
        A lead is \"qualified\" if it is an \"Auto Repair\" or \"Auto Detailing\" business located in \"Birmingham\".
        Respond ONLY with \"qualified\" or \"unqualified\".

        Lead details:
        Business Type: {business_type}
        City: {city}
        """
        # Use ask_groq instead of ask_claude
        response = ask_groq(
            system="You are a helpful AI assistant. Always respond concisely.",
            user=prompt,
            max_tokens=10
        )
        print(f"[DEBUG_GROQ] Groq's final text response: {response}")
        status = response.strip().lower()
        print(f"[DEBUG_GROQ] Processed status from Groq: {status}")

        if status == "qualified":
            lead["status"] = "Qualified"
            b44_update("Lead", lead["id"], {"status": "Qualified"})
            qualified_leads.append(lead)
            print(f"\u2b50 Qualified Lead: {lead['name']} ({business_type} in {city})")
        else:
            lead["status"] = "Unqualified"
            b44_update("Lead", lead["id"], {"status": "Unqualified"})
            print(f"\u274c Unqualified Lead: {lead['name']} ({business_type} in {city})")
    print("\u2705 [AGENT 2] Lead Qualification Complete.")
    return qualified_leads

def agent_assign_leads(leads_to_assign):
    print("\n\u2795 [AGENT 3] Lead Assignment Starting...")

    qualified_leads = []
    for lead in leads_to_assign:
        current_status = lead.get("status")
        current_business_type = lead.get("business_type")
        current_city = lead.get("city")

        # Filter for 'Qualified' leads that have business_type and city fields
        if current_status == "Qualified" and current_business_type and current_city:
             qualified_leads.append(lead)
        else:
            print(f"[WARN] Lead {lead.get('name', 'N/A')} skipped for assignment due to missing 'Qualified' status, business_type, or city.")

    print(f"[DEBUG] Leads identified for assignment: {len(qualified_leads)}")

    assigned_leads = []
    if not qualified_leads:
        print("No qualified leads to assign.")
        return assigned_leads

    sales_reps = [
        {"id": "rep_1", "name": "Alice"},
        {"id": "rep_2", "name": "Bob"}
    ]
    for i, lead in enumerate(qualified_leads):
        rep = sales_reps[i % len(sales_reps)]
        lead["assigned_to"] = rep["name"]
        lead["status"] = "Assigned"
        b44_update("Lead", lead["id"], {"assigned_to": rep["name"], "status": "Assigned"})
        assigned_leads.append(lead)
        print(f"\u2192 Assigned Lead: {lead['name']} to {rep['name']}")
    print("\u2705 [AGENT 3] Lead Assignment Complete.")
    return assigned_leads

async def run_agents():
    print("\U0001f680 Starting Leadflow Automation...")
    if not GROQ_API_KEY:
        print("GROQ_API_KEY environment variable is not set. Please set it before running.")
        return
    if not BASE44_API_KEY or not BASE44_APP_ID:
        print("BASE44_API_KEY or BASE44_APP_ID environment variables are not set. Please set them before running.")
        return

    # Explicit Groq API key and model access test
    if GROQ_API_KEY:
        print("\nAttempting a simple Groq API test call to verify API key and model access...")
        try:
            test_response = ask_groq(system="You are a helpful assistant.", user="Hello?")
            print(f"\u2705 Groq API test successful. Response: {test_response}")
        except Exception as e:
            print(f"\u274c Groq API test failed with an unexpected error: {e}")
            print("Please ensure your GROQ_API_KEY is correct and has access to the specified model (llama-3.1-8b-instant).")
            return

    captured = agent_capture_leads()
    time.sleep(2) # Give Base44 a moment to process the creations

    qualified = agent_qualify_leads(captured)
    assigned = agent_assign_leads(qualified)

    print("\n--- Leadflow Summary ---")
    print(f"Total Captured Leads: {len(captured)}")
    print(f"Total Qualified Leads: {len(qualified)}")
    print(f"Total Assigned Leads: {len(assigned)}")
    print("\U0001f3c1 Leadflow Automation Finished.")

if __name__ == "__main__":
    nest_asyncio.apply()
    asyncio.run(run_agents())
