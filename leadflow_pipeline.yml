name: LeadFlow AI v3 — Daily Pipeline

on:
  schedule:
    - cron: "0 15 * * 1-5"      # 9:00 AM CST (UTC-6) Mon-Fri
  workflow_dispatch:              # manual trigger from GitHub UI
    inputs:
      dry_run:
        description: "Dry run (skip SMS/Slack/CRM writes)"
        required: false
        default: "false"
        type: choice
        options: ["false", "true"]
      city_filter:
        description: "Optional: restrict to one city (e.g. Birmingham AL)"
        required: false
        default: ""

concurrency:
  group: leadflow-pipeline
  cancel-in-progress: false      # never cancel a mid-run pipeline

jobs:
  run-pipeline:
    name: Run LeadFlow AI v3
    runs-on: ubuntu-latest
    timeout-minutes: 45

    steps:
      # ── 1. Checkout ──────────────────────────────────────────────────────
      - name: Checkout repository
        uses: actions/checkout@v4

      # ── 2. Python setup with dependency caching ──────────────────────────
      - name: Set up Python 3.11
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"
          cache: "pip"

      # ── 3. Install dependencies ──────────────────────────────────────────
      - name: Install dependencies
        run: |
          pip install --upgrade pip
          pip install -r requirements.txt

      # ── 4. Validate secrets are present before running ───────────────────
      - name: Verify required secrets
        run: |
          MISSING=0
          for VAR in GROQ_API_KEY SERPAPI_API_KEY BASE44_API_KEY BASE44_APP_ID \
                     TWILIO_ACCOUNT_SID TWILIO_AUTH_TOKEN TWILIO_PHONE_NUMBER ALERT_PHONE_NUMBER; do
            if [ -z "${!VAR}" ]; then
              echo "ERROR: Missing secret — $VAR"
              MISSING=1
            else
              echo "OK: $VAR"
            fi
          done
          if [ "$MISSING" -eq 1 ]; then exit 1; fi
        env:
          GROQ_API_KEY:         ${{ secrets.GROQ_API_KEY }}
          SERPAPI_API_KEY:      ${{ secrets.SERPAPI_API_KEY }}
          BASE44_API_KEY:       ${{ secrets.BASE44_API_KEY }}
          BASE44_APP_ID:        ${{ secrets.BASE44_APP_ID }}
          TWILIO_ACCOUNT_SID:   ${{ secrets.TWILIO_ACCOUNT_SID }}
          TWILIO_AUTH_TOKEN:    ${{ secrets.TWILIO_AUTH_TOKEN }}
          TWILIO_PHONE_NUMBER:  ${{ secrets.TWILIO_PHONE_NUMBER }}
          ALERT_PHONE_NUMBER:   ${{ secrets.ALERT_PHONE_NUMBER }}

      # ── 5. Run the pipeline ──────────────────────────────────────────────
      - name: Run LeadFlow AI pipeline
        env:
          GROQ_API_KEY:         ${{ secrets.GROQ_API_KEY }}
          SERPAPI_API_KEY:      ${{ secrets.SERPAPI_API_KEY }}
          BASE44_API_KEY:       ${{ secrets.BASE44_API_KEY }}
          BASE44_APP_ID:        ${{ secrets.BASE44_APP_ID }}
          TWILIO_ACCOUNT_SID:   ${{ secrets.TWILIO_ACCOUNT_SID }}
          TWILIO_AUTH_TOKEN:    ${{ secrets.TWILIO_AUTH_TOKEN }}
          TWILIO_PHONE_NUMBER:  ${{ secrets.TWILIO_PHONE_NUMBER }}
          ALERT_PHONE_NUMBER:   ${{ secrets.ALERT_PHONE_NUMBER }}
          SLACK_WEBHOOK_URL:    ${{ secrets.SLACK_WEBHOOK_URL }}
          CRM_WEBHOOK_URL:      ${{ secrets.CRM_WEBHOOK_URL }}
          GROQ_MODEL:           llama-3.3-70b-versatile
          BASE44_ENTITY:        Lead
          DRY_RUN:              ${{ github.event.inputs.dry_run || 'false' }}
          CITY_FILTER:          ${{ github.event.inputs.city_filter || '' }}
        run: python leadflow_agent_v3.py

      # ── 6. Upload full run log (always, even on failure) ─────────────────
      - name: Upload run log
        if: always()
        uses: actions/upload-artifact@v4
        with:
          name: leadflow-log-${{ github.run_number }}-${{ github.run_attempt }}
          path: "*.log"
          retention-days: 90

      # ── 7. Post failure alert to Slack ───────────────────────────────────
      - name: Notify Slack on failure
        if: failure()
        run: |
          curl -s -X POST "$SLACK_WEBHOOK_URL" \
            -H "Content-Type: application/json" \
            -d "{
              \"text\": \":red_circle: *LeadFlow AI v3 FAILED* on run #${{ github.run_number }}\nBranch: \`${{ github.ref_name }}\`\nCheck logs: ${{ github.server_url }}/${{ github.repository }}/actions/runs/${{ github.run_id }}\"
            }"
        env:
          SLACK_WEBHOOK_URL: ${{ secrets.SLACK_WEBHOOK_URL }}
        continue-on-error: true
