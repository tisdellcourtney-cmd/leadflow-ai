import schedule
import time
import os
import threading
from datetime import datetime
from flask import Flask

app = Flask(__name__)

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

def scheduler_loop():
    run_agent()
    schedule.every().day.at("15:00").do(run_agent)  # 9:00 AM CST
    print("🕐 Scheduler active - running daily at 9:00 AM CST")
    while True:
        schedule.run_pending()
        time.sleep(60)

@app.route("/")
def home():
    return "LeadFlow AI is running!"

if __name__ == "__main__":
    thread = threading.Thread(target=scheduler_loop, daemon=True)
    thread.start()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
