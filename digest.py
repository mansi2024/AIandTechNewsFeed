"""Daily AI / Tech / Markets news digest, delivered by email.

Pipeline: fetch RSS -> dedupe + rank -> (optional) Gemini summaries -> HTML email.
If the LLM is unavailable the digest still goes out using the feed snippets.

Usage:
    python digest.py --dry-run     # writes digest.html, sends nothing
    python digest.py               # sends the email
"""
from __future__ import annotations

import argparse
import html
import json
import os
import re
import smtplib
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import feedparser
import requests

ROOT = Path(__file__).parent
SEEN_FILE = ROOT / "seen.json"
IST = timezone(timedelta(hours=5, minutes=30))

LOOKBACK_HOURS = 30
SEEN_RETENTION_DAYS = 14
ITEMS_PER_SECTION = 5
CANDIDATES_PER_SECTION = 10
GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-2.5-flash")

# Section -> list of (source name, feed url, weight). Weight nudges ranking toward
# sources you trust more; dead feeds are skipped with a warning, never fatal.
FEEDS = {
    "AI": [
        ("TechCrunch AI", "https://techcrunch.com/category/artificial-intelligence/feed/", 1.0),
        ("MIT Tech Review", "https://www.technologyreview.com/feed/", 1.0),
        ("VentureBeat AI", "https://venturebeat.com/category/ai/feed/", 0.8),
        ("OpenAI", "https://openai.com/news/rss.xml", 1.5),
        ("Google DeepMind", "https://deepmind.google/blog/rss.xml", 1.5),
        ("Hugging Face", "https://huggingface.co/blog/feed.xml", 1.0),
        ("Hacker News", "https://hnrss.org/frontpage?points=200&q=AI+OR+LLM+OR+OpenAI+OR+Anthropic+OR+Gemini", 1.2),
    ],
    "Tech": [
        ("The Verge", "https://www.theverge.com/rss/index.xml", 1.0),
        ("Ars Technica", "https://feeds.arstechnica.com/arstechnica/index", 1.0),
        ("TechCrunch", "https://techcrunch.com/feed/", 1.0),
        ("Wired", "https://www.wired.com/feed/rss", 0.8),
        ("Hacker News", "https://hnrss.org/frontpage?points=250", 1.2),
    ],
    "Global Markets & Economy": [
        ("CNBC", "https://www.cnbc.com/id/100003114/device/rss/rss.html", 1.2),
        ("CNBC Finance", "https://www.cnbc.com/id/10000664/device/rss/rss.html", 1.0),
        ("MarketWatch", "https://feeds.content.dowjones.io/public/rss/mw_topstories", 1.2),
        ("BBC Business", "http://feeds.bbci.co.uk/news/business/rss.xml", 1.0),
        ("Guardian Business", "https://www.theguardian.com/business/rss", 0.8),
    ],
    "India Markets & Economy": [
        ("Economic Times", "https://economictimes.indiatimes.com/markets/rssfeeds/1977021501.cms", 1.2),
        ("ET Top Stories", "https://economictimes.indiatimes.com/rssfeedstopstories.cms", 1.0),
        ("Mint", "https://www.livemint.com/rss/markets", 1.2),
        ("Moneycontrol", "https://www.moneycontrol.com/rss/MCtopnews.xml", 1.0),
        ("Business Standard", "https://www.business-standard.com/rss/markets-106.rss", 1.0),
        ("BusinessLine", "https://www.thehindubusinessline.com/markets/feeder/default.rss", 1.0),
    ],
}

MARKET_TICKERS = [
    ("S&P 500", "^GSPC"),
    ("Nasdaq", "^IXIC"),
    ("Dow", "^DJI"),
    ("Nifty 50", "^NSEI"),
    ("Sensex", "^BSESN"),
    ("USD/INR", "USDINR=X"),
    ("US 10Y yield", "^TNX"),
    ("Gold", "GC=F"),
    ("Brent", "BZ=F"),
    ("Bitcoin", "BTC-USD"),
]

STOPWORDS = set(
    "a an the and or of to in on for with at by from as is are was were be this that it its "
    "after before over into new says say said how why what will could may more than".split()
)


def log(msg: str) -> None:
    print(msg, file=sys.stderr)


# ---------------------------------------------------------------- collection

def clean_text(raw: str, limit: int = 400) -> str:
    text = re.sub(r"<[^>]+>", " ", raw or "")
    text = html.unescape(re.sub(r"\s+", " ", text)).strip()
    return text[:limit]


def canonical_url(url: str) -> str:
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path.rstrip("/"), "", ""))


def entry_time(entry) -> datetime | None:
    for key in ("published_parsed", "updated_parsed"):
        value = entry.get(key)
        if value:
            return datetime(*value[:6], tzinfo=timezone.utc)
    return None


def fetch_feed(job):
    section, source, url, weight = job
    try:
        resp = requests.get(url, timeout=20, headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"})
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
    except Exception as exc:  # one bad feed must not sink the run
        log(f"  ! {source}: {exc}")
        return []

    cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
    items = []
    for e in parsed.entries:
        when = entry_time(e)
        link = e.get("link")
        title = clean_text(e.get("title", ""), 200)
        if not link or not title or (when and when < cutoff):
            continue
        items.append({
            "section": section,
            "source": source,
            "weight": weight,
            "title": title,
            "url": link,
            "snippet": clean_text(e.get("summary") or e.get("description", "")),
            "time": when or datetime.now(timezone.utc),
        })
    log(f"  {source}: {len(items)} recent")
    return items


def collect() -> list[dict]:
    jobs = [(sec, name, url, w) for sec, feeds in FEEDS.items() for name, url, w in feeds]
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = pool.map(fetch_feed, jobs)
    return [item for batch in results for item in batch]


# -------------------------------------------------------------- dedupe / rank

def tokens(title: str) -> set[str]:
    return {w for w in re.findall(r"[a-z0-9]+", title.lower()) if w not in STOPWORDS and len(w) > 2}


def cluster(items: list[dict]) -> list[dict]:
    """Group near-duplicate headlines. A story covered by many sources ranks higher."""
    clusters: list[dict] = []
    for item in sorted(items, key=lambda i: i["time"], reverse=True):
        toks = tokens(item["title"])
        match = None
        for c in clusters:
            inter = len(toks & c["tokens"])
            union = len(toks | c["tokens"]) or 1
            if inter / union >= 0.5 or canonical_url(item["url"]) in c["urls"]:
                match = c
                break
        if match:
            match["items"].append(item)
            match["urls"].add(canonical_url(item["url"]))
            match["tokens"] |= toks
        else:
            clusters.append({"items": [item], "tokens": toks, "urls": {canonical_url(item["url"])}})

    now = datetime.now(timezone.utc)
    out = []
    for c in clusters:
        best = max(c["items"], key=lambda i: i["weight"])
        sources = {i["source"] for i in c["items"]}
        age_h = (now - best["time"]).total_seconds() / 3600
        score = 3 * len(sources) + best["weight"] + 2 * max(0.0, 1 - age_h / LOOKBACK_HOURS)
        out.append({
            "section": best["section"],
            "title": best["title"],
            "url": best["url"],
            "source": best["source"],
            "also": sorted(sources - {best["source"]}),
            "snippet": max((i["snippet"] for i in c["items"]), key=len),
            "urls": c["urls"],
            "score": score,
        })
    return out


def load_seen() -> dict:
    try:
        return json.loads(SEEN_FILE.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_seen(seen: dict) -> None:
    cutoff = (datetime.now(timezone.utc) - timedelta(days=SEEN_RETENTION_DAYS)).isoformat()
    kept = {u: t for u, t in seen.items() if t >= cutoff}
    SEEN_FILE.write_text(json.dumps(kept, indent=0, sort_keys=True))


def pick_candidates(clusters: list[dict], seen: dict) -> dict[str, list[dict]]:
    fresh = [c for c in clusters if not (c["urls"] & seen.keys())]
    by_section: dict[str, list[dict]] = {s: [] for s in FEEDS}
    for c in sorted(fresh, key=lambda c: c["score"], reverse=True):
        if len(by_section[c["section"]]) < CANDIDATES_PER_SECTION:
            by_section[c["section"]].append(c)
    return by_section


# ------------------------------------------------------------------ summaries

def gemini_summarize(candidates: dict[str, list[dict]]) -> dict | None:
    """Ask Gemini to choose the top stories per section and summarise them.

    Returns {"headline": str, "sections": {name: [{"id": int, "summary": str}]}} or None.
    """
    key = os.environ.get("GEMINI_API_KEY")
    if not key:
        log("No GEMINI_API_KEY; using feed snippets.")
        return None

    payload_sections = {
        sec: [
            {"id": i, "title": c["title"], "source": c["source"],
             "covered_by": len(c["also"]) + 1, "snippet": c["snippet"][:300]}
            for i, c in enumerate(cs)
        ]
        for sec, cs in candidates.items() if cs
    }
    prompt = (
        "You are the editor of a daily digest for a busy reader who follows AI, tech, "
        "global markets and the Indian economy. For each section below, choose the "
        f"{ITEMS_PER_SECTION} most important stories (market-moving, major product/research "
        "releases, policy shifts; skip clickbait, opinion fluff and minor items) ordered by "
        "importance. For each write a 1-2 sentence summary: what happened and why it matters. "
        "Use only facts present in the title/snippet; do not invent numbers.\n"
        "Also write 'headline': one sentence capturing the day's overall picture.\n"
        'Return JSON: {"headline": str, "sections": {"<section name>": [{"id": int, "summary": str}]}}\n\n'
        + json.dumps(payload_sections, ensure_ascii=False)
    )
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    body = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseMimeType": "application/json", "temperature": 0.3},
    }
    for attempt in range(3):
        try:
            resp = requests.post(url, json=body, headers={"x-goog-api-key": key}, timeout=90)
            if resp.status_code in (429, 500, 503):
                time.sleep(10 * (attempt + 1))
                continue
            resp.raise_for_status()
            text = resp.json()["candidates"][0]["content"]["parts"][0]["text"]
            return json.loads(text)
        except Exception as exc:
            log(f"Gemini attempt {attempt + 1} failed: {exc}")
            time.sleep(5)
    return None


def build_sections(candidates: dict[str, list[dict]], llm: dict | None) -> tuple[str, dict[str, list[dict]]]:
    headline = ""
    result: dict[str, list[dict]] = {}
    llm_sections = (llm or {}).get("sections", {}) if isinstance(llm, dict) else {}
    if isinstance(llm, dict):
        headline = str(llm.get("headline", ""))

    for sec, cs in candidates.items():
        chosen: list[dict] = []
        used: set[int] = set()
        for pick in llm_sections.get(sec, []):
            idx = pick.get("id") if isinstance(pick, dict) else None
            if isinstance(idx, int) and 0 <= idx < len(cs) and idx not in used:
                used.add(idx)
                chosen.append({**cs[idx], "summary": str(pick.get("summary", "")).strip()})
        for idx, c in enumerate(cs):  # top up / full fallback
            if len(chosen) >= ITEMS_PER_SECTION:
                break
            if idx not in used:
                chosen.append({**c, "summary": c["snippet"][:220]})
        result[sec] = chosen[:ITEMS_PER_SECTION]
    return headline, result


# -------------------------------------------------------------------- markets

def market_snapshot() -> list[dict]:
    try:
        import yfinance as yf
    except ImportError:
        return []
    rows = []
    for label, symbol in MARKET_TICKERS:
        try:
            hist = yf.Ticker(symbol).history(period="5d")["Close"].dropna()
            if len(hist) < 2:
                continue
            last, prev = float(hist.iloc[-1]), float(hist.iloc[-2])
            rows.append({"label": label, "value": last, "pct": (last / prev - 1) * 100})
        except Exception as exc:
            log(f"  ! {label}: {exc}")
    return rows


# ---------------------------------------------------------------------- email

def render_html(headline: str, sections: dict[str, list[dict]], markets: list[dict]) -> str:
    esc = html.escape
    today = datetime.now(IST).strftime("%A, %d %B %Y")
    parts = [
        '<div style="font-family:-apple-system,Segoe UI,Roboto,Arial,sans-serif;max-width:640px;'
        'margin:0 auto;padding:16px;color:#1a1a1a;line-height:1.5">',
        f'<h1 style="font-size:22px;margin:0 0 4px">Daily Digest</h1>'
        f'<div style="color:#666;font-size:13px;margin-bottom:12px">{today}</div>',
    ]
    if headline:
        parts.append(f'<p style="font-size:16px;margin:0 0 16px"><b>{esc(headline)}</b></p>')

    if markets:
        cells = []
        for m in markets:
            color = "#0a7d36" if m["pct"] >= 0 else "#c0392b"
            cells.append(
                '<td style="padding:6px 10px;border-bottom:1px solid #eee;font-size:13px">'
                f'{esc(m["label"])}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #eee;font-size:13px;text-align:right">'
                f'{m["value"]:,.2f}</td>'
                f'<td style="padding:6px 10px;border-bottom:1px solid #eee;font-size:13px;text-align:right;'
                f'color:{color}">{m["pct"]:+.2f}%</td>'
            )
        rows = "".join(f"<tr>{c}</tr>" for c in cells)
        parts.append(
            '<h2 style="font-size:16px;margin:16px 0 6px;border-bottom:2px solid #1a1a1a;padding-bottom:4px">'
            f'Markets snapshot</h2><table style="width:100%;border-collapse:collapse">{rows}</table>'
        )

    for sec, items in sections.items():
        if not items:
            continue
        parts.append(
            '<h2 style="font-size:16px;margin:24px 0 8px;border-bottom:2px solid #1a1a1a;padding-bottom:4px">'
            f"{esc(sec)}</h2>"
        )
        for it in items:
            also = f" +{len(it['also'])} more" if it["also"] else ""
            parts.append(
                '<div style="margin:0 0 14px">'
                f'<a href="{esc(it["url"])}" style="font-size:15px;font-weight:600;color:#0b57d0;'
                f'text-decoration:none">{esc(it["title"])}</a>'
                f'<div style="font-size:14px;margin-top:2px">{esc(it["summary"])}</div>'
                f'<div style="font-size:12px;color:#888;margin-top:2px">{esc(it["source"])}{esc(also)}</div>'
                "</div>"
            )
    parts.append('<p style="font-size:11px;color:#999;margin-top:28px">Generated automatically.</p></div>')
    return "".join(parts)


def send_email(subject: str, body_html: str) -> None:
    user = os.environ["GMAIL_ADDRESS"].strip()
    password = "".join(os.environ["GMAIL_APP_PASSWORD"].split())  # drop spaces/newlines from pasted secret
    to = (os.environ.get("TO_EMAIL") or "").strip() or user
    msg = MIMEMultipart("alternative")
    msg["Subject"], msg["From"], msg["To"] = subject, user, to
    msg.attach(MIMEText("Open this email in an HTML-capable client.", "plain"))
    msg.attach(MIMEText(body_html, "html"))
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as smtp:
        smtp.login(user, password)
        smtp.sendmail(user, [to], msg.as_string())


# ----------------------------------------------------------------------- main

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true", help="write digest.html instead of emailing")
    args = ap.parse_args()

    log("Fetching feeds...")
    items = collect()
    if not items:
        log("No items fetched; aborting.")
        return 1

    seen = load_seen()
    candidates = pick_candidates(cluster(items), seen)
    if not any(candidates.values()):
        log("Nothing new since last digest.")
        return 0

    log("Summarising...")
    headline, sections = build_sections(candidates, gemini_summarize(candidates))
    log("Market snapshot...")
    body = render_html(headline, sections, market_snapshot())
    subject = f"Daily Digest - {datetime.now(IST).strftime('%d %b %Y')}"

    if args.dry_run:
        (ROOT / "digest.html").write_text(body)
        log("Wrote digest.html (dry run, seen.json untouched).")
        return 0

    send_email(subject, body)
    now = datetime.now(timezone.utc).isoformat()
    for items_ in sections.values():
        for it in items_:
            for u in it["urls"]:
                seen[u] = now
    save_seen(seen)
    log("Sent.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
