import schedule
import time
import os
from datetime import datetime

def run_agent():
    print(f"\n{'='*50}")
    print(f"🚀 LeadFlow Agent starting: {datetime.now()}")
    print(f"{'='*50}")
    try:
        with open("leadflow_agent.py", "r") as f:
            code = f.read()
        exec(compile(code, "leadflow_agent.py", "exec"), {})
        print(f"✅ Agent completed: {datetime.now()}")
    except Exception as e:
        print(f"❌ Agent failed: {e}")

# Run immediately on startup
print("🚀 LeadFlow AI Starting...")
run_agent()

# Schedule daily at 9AM
schedule.every().day.at("09:00").do(run_agent)
print("⏰ Scheduled to run daily at 9:00 AM")

while True:
    schedule.run_pending()
    time.sleep(60)
