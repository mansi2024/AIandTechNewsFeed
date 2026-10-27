# Daily AI / Tech / Markets digest

Emails one briefing a day (10:00 IST): AI, Tech, Global Markets & Economy, India Markets & Economy, plus a markets snapshot.
Gemini (free tier) picks and summarises the top stories; if it fails, the email still goes out with feed snippets.

## Setup
1. Create a GitHub repo and push this folder.
2. Repo -> Settings -> Secrets and variables -> Actions -> add:
   - `GEMINI_API_KEY` (free, https://aistudio.google.com/apikey)
   - `GMAIL_ADDRESS` (the sending Gmail)
   - `GMAIL_APP_PASSWORD` (Google Account -> Security -> 2-Step Verification -> App passwords)
   - `TO_EMAIL` (optional; defaults to GMAIL_ADDRESS)
3. Actions tab -> "Daily digest" -> Run workflow to test.

## Local
    python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    GEMINI_API_KEY=... .venv/bin/python digest.py --dry-run   # writes digest.html

Edit `FEEDS` in `digest.py` to add/remove sources; change the cron in `.github/workflows/daily.yml` for another time.
