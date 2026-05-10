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
        exec(code, globals())
        print(f"✅ Agent completed successfully: {datetime.now()}")
    except Exception as e:
        print(f"❌ Agent failed: {e}")

# Run once immediately on startup
run_agent()

# Then run every day at 9:00 AM
schedule.every().day.at("09:00").do(run_agent)

print("⏰ Scheduler active - LeadFlow Agent will run daily at 9:00 AM")

while True:
    schedule.run_pending()
    time.sleep(60)
