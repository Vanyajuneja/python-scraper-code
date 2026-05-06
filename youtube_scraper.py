"""
YouTube Comment Pain-Point Lead Scraper
========================================
Scrapes comments from target Indian medical YouTube channels and search queries.
Uses scrapetube (video discovery) + youtube-comment-downloader (comment extraction).
No API key required.

Usage:
    python youtube_scraper.py
    python youtube_scraper.py --max-videos 5 --max-comments 200
    python youtube_scraper.py --search-only
    python youtube_scraper.py --channels-only
"""

import sys
import io
import json
import re
import time
import argparse
from datetime import datetime, timezone
from collections import Counter

try:
    from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
    sentiment_analyzer = SentimentIntensityAnalyzer()
except ImportError:
    print("[ERROR] Missing dependency: pip install vaderSentiment")
    sys.exit(1)

# Force UTF-8 output on Windows
if sys.platform == "win32":
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

try:
    import scrapetube
except ImportError:
    print("[ERROR] Missing dependency: pip install scrapetube")
    sys.exit(1)

try:
    from youtube_comment_downloader import YoutubeCommentDownloader
    from youtube_comment_downloader.downloader import SORT_BY_RECENT
except ImportError:
    print("[ERROR] Missing dependency: pip install youtube-comment-downloader")
    sys.exit(1)


# ─── Configuration ───────────────────────────────────────────────────────────

TARGET_CHANNELS = [
    # (handle_or_url, display_name)
    ("@DrAnujPachhel", "Dr Anuj Pachhel"),
    ("@mitali.this.side", "Mitali"),
    ("@DoctorAni", "Doctor Ani"),
    ("@PoorviSachan", "Poorvi Sachan"),
    ("@AdvikaSingh", "Advika Singh"),
    ("@DocRocks", "DocRocks"),
]

SEARCH_QUERIES = [
    "First 24-hour internship duty vlog India",
    "NEET PG hell week",
    "MBBS Intern Vlog in COVID Ward",
    "MBBS internship burnout India",
    "Indian medical student struggle vlog",
    "PG residency life India vlog",
    "MBBS hostel life India",
    "medical college ragging India",
    "NEET PG study burnout",
    "Indian doctor night shift vlog",
]

INHERENTLY_NEGATIVE_KEYWORDS = [
    "toxic senior", "verbal abuse", "humiliation", "burnout", "quit medicine",
    "guide not helping", "stipend delay", "unpaid", "app crash", "slow ui",
    "no customer support", "blame game", "credit stealing", "fear of consultant",
    "panic attacks", "imposter syndrome", "emotional exhaustion",
    "sleep deprivation", "insomnia", "skipping meals", "angry relatives",
    "violence risk", "negligence anxiety", "toxic competition", "ragging",
    "scut work", "no sleep"
]

NEGATIVE_MODIFIERS = [
    "tired", "exhaust", "depress", "struggl", "hard", "tough", "bad",
    "worst", "hate", "cry", "sad", "fail", "fear", "scare", "anxious",
    "anxiety", "pressure", "toxic", "quit", "suck", "ruin", "stress",
    "frustrat", "overwhelm", "give up", "hopeless", "breakdown",
    "hell", "pain", "unfair", "regret", "torture", "harass", "suicide",
    "drain", "sleepless", "lonely", "no life"
]

PAIN_POINTS = {
    "Documentation": [
        "discharge summary", "LAMA", "DAMA", "case summary",
        "logbook", "paperwork",
    ],
    "Workload": [
        "24 hour duty", "36 hour", "night shift", "scut work",
        "IV line", "foley", "no sleep", "patient load",
    ],
    "Toxic Culture": [
        "ragging", "toxic senior", "verbal abuse", "humiliation",
        "burnout", "quit medicine",
    ],
    "Research/Thesis": [
        "SPSS", "thesis", "dissertation", "reproducible",
        "p-value", "guide not helping",
    ],
    "Exam Stress": [
        "NEET PG", "INI CET", "FMGE", "prof exam", "rank anxiety",
    ],
    "Financial": [
        "stipend delay", "bond", "unpaid",
    ],
    "Tech Friction": [
        "app crash", "EHR", "EMR", "slow UI",
        "no customer support", "voice to text",
    ],
    "Clinical Pressure": [
        "critical patient", "ICU stress", "emergency case", "code blue",
        "high risk consent", "death handling", "breaking bad news",
    ],
    "Hierarchy Issues": [
        "consultant pressure", "senior junior gap", "no autonomy",
        "fear of consultant", "blame game", "credit stealing",
    ],
    "Learning Gaps": [
        "no teaching", "lack of guidance", "missed concepts",
        "clinical confusion", "theory vs practice gap", "self study struggle",
    ],
    "Time Management": [
        "no time to study", "duty vs study", "poor schedule",
        "procrastination", "backlog", "time crunch",
    ],
    "Mental Health": [
        "anxiety", "depression", "panic attacks", "overthinking",
        "imposter syndrome", "emotional exhaustion",
    ],
    "Sleep Issues": [
        "sleep deprivation", "insomnia", "irregular sleep",
        "fatigue", "circadian disruption",
    ],
    "Physical Strain": [
        "back pain", "standing long hours", "leg pain",
        "no food break", "skipping meals", "weight loss",
    ],
    "Patient Interaction": [
        "difficult patient", "angry relatives", "violence risk",
        "communication barrier", "language issue", "non compliant patient",
    ],
    "Medico-Legal": [
        "legal case", "consent issue", "documentation fear",
        "court notice", "negligence anxiety",
    ],
    "Career Uncertainty": [
        "branch confusion", "future anxiety", "job insecurity",
        "private vs govt", "abroad vs india", "career switch",
    ],
    "Relationships & Social Life": [
        "no social life", "relationship strain", "family pressure",
        "missing events", "loneliness",
    ],
    "Hostel / Living Conditions": [
        "poor hostel", "mess food", "room issues",
        "hygiene problems", "no privacy",
    ],
    "Administrative Issues": [
        "attendance issue", "leave rejection", "rota problems",
        "HR delays", "mismanagement",
    ],
    "Skill Anxiety": [
        "first procedure fear", "injection anxiety", "surgical fear",
        "making mistakes", "low confidence",
    ],
    "Competition": [
        "peer comparison", "rank pressure", "toxic competition",
        "performance pressure",
    ],
    "Digital Overload": [
        "too many resources", "telegram overload", "youtube distraction",
        "note making burnout",
    ],
}


# ─── Keyword Matching ────────────────────────────────────────────────────────

def build_keyword_patterns():
    """Pre-compile case-insensitive regex patterns for each pain-point category."""
    patterns = {}
    for category, keywords in PAIN_POINTS.items():
        escaped = [re.escape(kw) for kw in keywords]
        patterns[category] = re.compile(
            r"\b(" + "|".join(escaped) + r")\b",
            re.IGNORECASE,
        )
    return patterns


def classify_text(text: str, patterns: dict) -> list[dict]:
    """Return matched pain-point categories and keywords found."""
    matches = []
    for category, pat in patterns.items():
        found = list(set(m.group(0).lower() for m in pat.finditer(text)))
        if found:
            matches.append({"category": category, "keywords_matched": found})
    return matches


# ─── Video Discovery ─────────────────────────────────────────────────────────

def discover_channel_videos(channel_handle: str, display_name: str,
                             max_videos: int = 10) -> list[dict]:
    """Get recent videos from a YouTube channel using scrapetube."""
    print(f"\n  [>] Discovering videos from {display_name} ({channel_handle}) ...")
    videos = []
    channel_url = f"https://www.youtube.com/{channel_handle}"

    try:
        for i, video in enumerate(scrapetube.get_channel(channel_url=channel_url)):
            if i >= max_videos:
                break
            vid_id = video.get("videoId", "")
            title = ""
            # scrapetube returns title in nested structure
            title_runs = video.get("title", {}).get("runs", [])
            if title_runs:
                title = title_runs[0].get("text", "")
            elif isinstance(video.get("title"), dict):
                title = video["title"].get("simpleText", "")

            videos.append({
                "video_id": vid_id,
                "title": title,
                "channel": display_name,
                "channel_handle": channel_handle,
                "url": f"https://www.youtube.com/watch?v={vid_id}",
            })
        print(f"      Found {len(videos)} videos")
    except Exception as exc:
        print(f"      [WARN] Could not fetch channel {channel_handle}: {exc}")

    return videos


def discover_search_videos(query: str, max_results: int = 5) -> list[dict]:
    """Search YouTube and return video metadata."""
    print(f"\n  [>] Searching: \"{query}\" ...")
    videos = []

    try:
        for i, video in enumerate(scrapetube.get_search(query)):
            if i >= max_results:
                break
            # scrapetube search can return non-video items (channels, playlists)
            if video.get("videoId") is None:
                continue

            vid_id = video["videoId"]
            title = ""
            title_runs = video.get("title", {}).get("runs", [])
            if title_runs:
                title = title_runs[0].get("text", "")

            channel_name = ""
            channel_runs = video.get("ownerText", {}).get("runs", [])
            if channel_runs:
                channel_name = channel_runs[0].get("text", "")

            videos.append({
                "video_id": vid_id,
                "title": title,
                "channel": channel_name,
                "channel_handle": "",
                "url": f"https://www.youtube.com/watch?v={vid_id}",
            })
        print(f"      Found {len(videos)} videos")
    except Exception as exc:
        print(f"      [WARN] Search failed for \"{query}\": {exc}")

    return videos


# ─── Comment Scraping ─────────────────────────────────────────────────────────

def is_complaint(text: str, matched_keywords: list) -> bool:
    """Check if the text represents a genuine negative pain point."""
    text_lower = text.lower()
    
    # 1. Inherently negative keyword found
    for kw_dict in matched_keywords:
        for kw in kw_dict.get("keywords_matched", []):
            if kw in INHERENTLY_NEGATIVE_KEYWORDS:
                return True
                
    # 2. Contains a negative modifier
    for mod in NEGATIVE_MODIFIERS:
        if mod in text_lower:
            return True
            
    # 3. VADER Sentiment is sufficiently negative
    score = sentiment_analyzer.polarity_scores(text)
    if score['compound'] <= -0.1:
        return True
        
    return False

def scrape_comments(video: dict, patterns: dict,
                     max_comments: int = 300) -> list[dict]:
    """
    Download comments for a single video and return leads matching keywords.
    """
    video_id = video["video_id"]
    video_title = video.get("title", "")
    video_url = video["url"]
    channel = video.get("channel", "")

    print(f"    [+] Comments: {video_title[:60]}... ", end="", flush=True)

    downloader = YoutubeCommentDownloader()
    leads = []
    comment_count = 0

    try:
        generator = downloader.get_comments_from_url(video_url, sort_by=SORT_BY_RECENT)

        for comment in generator:
            if comment_count >= max_comments:
                break
            comment_count += 1

            text = comment.get("text", "")
            if not text or len(text.strip()) < 10:
                continue

            matches = classify_text(text, patterns)
            if matches and is_complaint(text, matches):
                lead = build_lead_from_comment(
                    comment=comment,
                    video=video,
                    matches=matches,
                )
                leads.append(lead)

    except Exception as exc:
        print(f"[WARN] {exc}")
        return leads

    print(f"scanned {comment_count} | matched {len(leads)}")
    return leads


# ─── Lead Building ───────────────────────────────────────────────────────────

def build_lead_from_comment(comment: dict, video: dict,
                              matches: list[dict]) -> dict:
    """Build a Supabase-compatible lead dict from a YouTube comment."""
    author = comment.get("author", "unknown")
    text = (comment.get("text", "") or "").strip()
    comment_time = comment.get("time", "")
    votes = comment.get("votes", "0")
    is_reply = comment.get("reply", False)

    return {
        # Fields matching Supabase schema
        "user_id": author,
        "pain_point": text,                        # The actual comment text
        "email": None,
        "phone": None,
        "source": f"youtube:{video.get('channel', 'unknown')}",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "processed": False,

        # Extra metadata (strip before Supabase insert)
        "_meta": {
            "comment_id": comment.get("cid", ""),
            "video_id": video.get("video_id", ""),
            "video_title": video.get("title", ""),
            "video_url": video.get("url", ""),
            "channel": video.get("channel", ""),
            "channel_handle": video.get("channel_handle", ""),
            "comment_time_text": comment_time,     # e.g., "2 weeks ago"
            "votes": votes,
            "is_reply": is_reply,
            "content_type": "youtube_comment",
            "classification": matches,
        },
    }


# ─── Output Helpers ──────────────────────────────────────────────────────────

def strip_meta(leads: list[dict]) -> list[dict]:
    """Return a copy of leads without the _meta field (Supabase-ready)."""
    return [{k: v for k, v in lead.items() if k != "_meta"} for lead in leads]


def save_json(data, filepath: str):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False, default=str)
    print(f"[SAVED] {filepath}  ({len(data)} records)")


def print_summary(leads: list[dict]):
    """Print a summary table of leads by source and category."""
    sub_counts = Counter()
    cat_counts = Counter()
    for lead in leads:
        sub_counts[lead["source"]] += 1
        for m in lead.get("_meta", {}).get("classification", []):
            cat_counts[m["category"]] += 1

    print(f"\n{'='*60}")
    print(f"  YOUTUBE SCRAPE SUMMARY -- {len(leads)} total leads")
    print(f"{'='*60}")
    print("\n  By Channel/Source:")
    for src, cnt in sub_counts.most_common():
        print(f"    {src:<40} {cnt:>5}")
    print("\n  By Pain-Point Category:")
    for cat, cnt in cat_counts.most_common():
        print(f"    {cat:<40} {cnt:>5}")
    print()


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Scrape YouTube comments from Indian medical channels for pain-point leads."
    )
    parser.add_argument(
        "--max-videos", type=int, default=5,
        help="Max videos to scrape per channel (default: 5)"
    )
    parser.add_argument(
        "--max-search-results", type=int, default=3,
        help="Max videos per search query (default: 3)"
    )
    parser.add_argument(
        "--max-comments", type=int, default=300,
        help="Max comments to scan per video (default: 300)"
    )
    parser.add_argument(
        "--output", type=str, default="yt_leads_full.json",
        help="Output file (default: yt_leads_full.json)"
    )
    parser.add_argument(
        "--channels-only", action="store_true",
        help="Only scrape target channels (skip search queries)"
    )
    parser.add_argument(
        "--search-only", action="store_true",
        help="Only scrape search query results (skip channels)"
    )
    args = parser.parse_args()

    patterns = build_keyword_patterns()
    all_leads = []
    all_videos = []

    print("[*] YouTube Comment Pain-Point Lead Scraper")
    print(f"    Max videos/channel : {args.max_videos}")
    print(f"    Max search results : {args.max_search_results}")
    print(f"    Max comments/video : {args.max_comments}")
    print(f"    Output             : {args.output}")

    # ── Phase 1: Discover videos ──────────────────────────────────────────

    if not args.search_only:
        print(f"\n{'='*60}")
        print(f"  PHASE 1a: Channel Video Discovery ({len(TARGET_CHANNELS)} channels)")
        print(f"{'='*60}")

        for handle, name in TARGET_CHANNELS:
            videos = discover_channel_videos(handle, name, args.max_videos)
            all_videos.extend(videos)

    if not args.channels_only:
        print(f"\n{'='*60}")
        print(f"  PHASE 1b: Search Query Video Discovery ({len(SEARCH_QUERIES)} queries)")
        print(f"{'='*60}")

        for query in SEARCH_QUERIES:
            videos = discover_search_videos(query, args.max_search_results)
            all_videos.extend(videos)

    # Deduplicate videos by video_id
    seen_ids = set()
    unique_videos = []
    for v in all_videos:
        if v["video_id"] not in seen_ids:
            seen_ids.add(v["video_id"])
            unique_videos.append(v)

    print(f"\n  Total unique videos to scrape: {len(unique_videos)}")

    # ── Phase 2: Scrape comments ──────────────────────────────────────────

    print(f"\n{'='*60}")
    print(f"  PHASE 2: Comment Scraping ({len(unique_videos)} videos)")
    print(f"{'='*60}")

    for i, video in enumerate(unique_videos, 1):
        print(f"\n  [{i}/{len(unique_videos)}] {video['channel']}")
        video_leads = scrape_comments(video, patterns, args.max_comments)
        all_leads.extend(video_leads)

        # Brief pause between videos to be polite
        if i < len(unique_videos):
            time.sleep(1)

    # Deduplicate leads by comment_id
    seen_cids = set()
    unique_leads = []
    for lead in all_leads:
        cid = lead["_meta"]["comment_id"]
        if cid and cid not in seen_cids:
            seen_cids.add(cid)
            unique_leads.append(lead)
        elif not cid:
            unique_leads.append(lead)

    # ── Phase 3: Save results ─────────────────────────────────────────────

    save_json(unique_leads, args.output)

    supabase_file = args.output.replace(".json", "_supabase.json")
    save_json(strip_meta(unique_leads), supabase_file)

    print_summary(unique_leads)

    print("[DONE] Two files written:")
    print(f"    1. {args.output:<35}  <- full data + metadata")
    print(f"    2. {supabase_file:<35}  <- Supabase-ready (matches DB schema)")


if __name__ == "__main__":
    main()
