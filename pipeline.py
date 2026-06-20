#!/usr/bin/env python3
"""
pipeline.py  —  YouTube Collab SNA Pipeline
============================================

Consolidates three steps into one runnable script:

  Step 1 — SCRAPE  (no API key needed)
    Crawls YouTube watch/channel pages to find videos that use the
    Collaborators feature (≤5 credited channels per video). Builds a channel
    network stored in a SQLite database (--db).

  Step 2 — ENRICH  (requires --api-key)
    Queries the YouTube Data API v3 for every channel discovered in step 1.
    Adds topic categories, country, description, keyword tags, and exact counts
    (views, videos, subscribers). Results are written to the DB and are
    resumable — channels already enriched are skipped.

  Step 3 — EXPORT
    Reads the (optionally enriched) DB and writes two Gephi-ready CSV files:
      gephi_nodes.csv            — one row per channel (all enrichment columns
                                   are included if step 2 was run)
      gephi_edges_undirected.csv — one row per collaborating channel pair,
                                   with Weight = number of shared collab videos

All three steps run in sequence by default.  Use --scrape-only / --enrich-only /
--export-only to run just one step.

Usage
-----
  # Full pipeline (enrich requires API key):
  python pipeline.py --api-key YOUR_KEY

  # Just scrape — no API key needed:
  python pipeline.py --scrape-only

  # Enrich already-scraped data:
  python pipeline.py --enrich-only --api-key YOUR_KEY

  # Export already-scraped / already-enriched data:
  python pipeline.py --export-only

  # Resume an interrupted run:
  python pipeline.py --resume --api-key YOUR_KEY

  # Smaller test run (500 channels):
  python pipeline.py --target 500 --api-key YOUR_KEY

  # Reduce music/topic bias:
  python pipeline.py --low-bias --api-key YOUR_KEY

  # Seed crawl from a specific list of channels:
  python pipeline.py --seed-channels top_channels.txt --api-key YOUR_KEY
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import re
import sqlite3
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from itertools import combinations
from typing import Any, Iterable, Optional

import requests

# =============================================================================
# Constants
# =============================================================================

DEFAULT_TARGET_CHANNELS = 3000
DEFAULT_DB              = "collab_data.db"
DEFAULT_MIN_DELAY       = 1.5          # seconds between HTTP requests
DEFAULT_MAX_DELAY       = 3.5
UPLOADS_PER_CHANNEL     = 30           # recent uploads harvested per channel
REQUEST_TIMEOUT         = 20
MAX_RETRIES             = 4
MAX_COLLABORATORS       = 5            # YouTube's collab-feature hard cap
SEED_BATCH              = 8            # seed queries used per re-seed pass
STALE_LIMIT             = 60           # videos without new channel → re-seed

# Low-bias mode: trades reach for a flatter topic distribution
LOW_BIAS_UPLOADS_PER_CHANNEL = 10
LOW_BIAS_PER_UPLOADER_CAP    = 20
SEARCH_PAGES                 = 1
LOW_BIAS_SEARCH_PAGES        = 8

# YouTube Data API enrichment
API_BATCH_SIZE = 50
API_SLEEP_SEC  = 0.05

# Topic-neutral collab markers (used in low-bias mode instead of genre queries)
NEUTRAL_MARKERS = [
    "collab", "collaboration", "collabs", "we collabed", "collab video",
    "featuring", "feat", "ft", "duet", "joint video", "collaborated with",
    "colaboración", "colaboração", "participação especial", "collaborazione",
    "kollaboration", "samenwerking", "współpraca", "коллаборация",
    "совместный клип", "أغنية مشتركة", "تعاون فني", "कोलैब", "गाना feat",
    "コラボ", "콜라보", "컬래버", "kolaborasi", "ร่วมงาน", "düet", "kết hợp",
    "合作", "合作歌曲", "duo officiel", "canción colaboración",
]

# Trending feeds: topic-agnostic popularity seeds
TRENDING_URLS = [
    "https://www.youtube.com/feed/trending",
    "https://www.youtube.com/feed/trending?bp=4gIuCggvbS8wNHJsZhIiUEx1WTNxYWNFV0Z5RTAyMzZiUnp5Q3ZQWUNRZGVfbGE%3D",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    # Bypass consent wall
    "Cookie": "CONSENT=YES+cb; SOCS=CAI;",
}


def _build_seed_queries() -> list[str]:
    """~250 diverse collab-heavy queries covering music genres, non-music topics,
    and multiple languages — so the snowball crawl enters many disconnected clusters."""
    q: set[str] = set()
    genres = [
        "hip hop", "rap", "trap", "drill", "afrobeats", "amapiano", "reggaeton",
        "latin", "k-pop", "j-pop", "edm", "house", "techno", "pop", "r&b", "rnb",
        "country", "rock", "metal", "jazz", "gospel", "indie", "lofi", "folk",
        "punk", "reggae", "dancehall", "grime", "phonk", "hyperpop", "bollywood",
        "arabic music", "turkish music", "french rap", "german rap", "spanish music",
        "brazilian funk", "nigerian music", "ghana music",
    ]
    for g in genres:
        q.add(f"{g} feat")
        q.add(f"{g} collab")
    for fmt in ["official music video", "official video", "official audio",
                "music video", "song", "single"]:
        q.add(f"{fmt} ft")
        q.add(f"{fmt} feat")
    topics = [
        "gaming", "minecraft", "fortnite", "roblox", "valorant", "fifa",
        "podcast", "interview", "vlog", "challenge", "react", "cooking",
        "fitness", "makeup", "skincare", "dance", "comedy sketch", "animation",
        "tech review", "car build", "fishing", "travel", "fashion", "art",
        "drawing", "science", "diy", "football", "basketball", "kids", "asmr",
    ]
    for t in topics:
        q.add(f"{t} collab")
        q.add(f"{t} feat")
        q.add(f"{t} with")
    for mk in ("collab", "feat", "collaboration"):
        for y in ("2024", "2025", "2026"):
            q.add(f"{mk} {y}")
    q.update([
        "colaboracion oficial video", "feat video oficial", "musica colaboracion",
        "collaboration musique feat", "deutsch collab feat", "lagu kolaborasi",
        "コラボ 公式", "콜라보 뮤직비디오", "गाना feat", "música colaboração feat",
        "collaborazione musicale", "samenwerking video", "współpraca feat",
        "лучшая коллаборация", "أغنية مشتركة", "เพลงร่วม feat",
    ])
    return sorted(q)


SEED_QUERIES = _build_seed_queries()


# =============================================================================
# Utilities
# =============================================================================

def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def parse_count(text: Optional[str]) -> Optional[int]:
    """'1.2M subscribers' → 1200000; '12,345' → 12345; '907K' → 907000."""
    if not text:
        return None
    text = text.replace(",", "").strip()
    m = re.search(r"([\d.]+)\s*([KMB])?", text, re.IGNORECASE)
    if not m:
        return None
    num = float(m.group(1))
    suffix = (m.group(2) or "").upper()
    mult = {"": 1, "K": 1_000, "M": 1_000_000, "B": 1_000_000_000}[suffix]
    return int(num * mult)


def walk(obj: Any) -> Iterable[Any]:
    """Depth-first traversal of any nested JSON structure."""
    stack = [obj]
    while stack:
        node = stack.pop()
        yield node
        if isinstance(node, dict):
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)


def _re1(pattern: str, text: str) -> Optional[str]:
    m = re.search(pattern, text)
    return m.group(1) if m else None


def first_text(node: Any) -> Optional[str]:
    """Extract text from YouTube's {'simpleText': …} or {'runs': […]} nodes."""
    if isinstance(node, str):
        return node
    if isinstance(node, dict):
        if "simpleText" in node:
            return node["simpleText"]
        if "runs" in node and isinstance(node["runs"], list):
            return "".join(r.get("text", "") for r in node["runs"])
    return None


def find_continuation_token(obj: Any) -> Optional[str]:
    for n in walk(obj):
        if isinstance(n, dict):
            cc = n.get("continuationCommand")
            if isinstance(cc, dict) and cc.get("token"):
                return cc["token"]
    return None


# =============================================================================
# HTTP Fetcher
# =============================================================================

class Fetcher:
    def __init__(self, min_delay: float, max_delay: float,
                 proxy: Optional[str] = None):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        if proxy:
            self.session.proxies.update({"http": proxy, "https": proxy})
        self.min_delay = min_delay
        self.max_delay = max_delay
        self._last = 0.0

    def _throttle(self) -> None:
        elapsed = time.time() - self._last
        wait = random.uniform(self.min_delay, self.max_delay) - elapsed
        if wait > 0:
            time.sleep(wait)
        self._last = time.time()

    def get(self, url: str) -> Optional[str]:
        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            except requests.RequestException as e:
                log(f"  request error ({attempt}/{MAX_RETRIES}) {url}: {e}")
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 200:
                return resp.text
            if resp.status_code == 429:
                backoff = 10 * attempt
                log(f"  429 rate-limited — backing off {backoff}s...")
                time.sleep(backoff)
                continue
            log(f"  HTTP {resp.status_code} on {url}")
            return None
        return None

    def post_json(self, url: str, body: dict) -> Optional[dict]:
        """POST to a youtubei continuation endpoint."""
        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()
            try:
                resp = self.session.post(url, json=body, timeout=REQUEST_TIMEOUT)
            except requests.RequestException:
                time.sleep(2 ** attempt)
                continue
            if resp.status_code == 200:
                try:
                    return resp.json()
                except ValueError:
                    return None
            if resp.status_code == 429:
                time.sleep(10 * attempt)
                continue
            return None
        return None


# =============================================================================
# YouTube page parsers
# =============================================================================

def extract_json_var(html: str, var_name: str) -> Optional[dict]:
    """Pull an embedded JSON object (e.g. ytInitialData) out of page HTML."""
    patterns = [
        var_name + r"\s*=\s*(\{.*?\})\s*;\s*</script>",
        var_name + r"\s*=\s*(\{.*?\})\s*;",
    ]
    for pat in patterns:
        m = re.search(pat, html, re.DOTALL)
        if m:
            for end in _balanced_ends(html, m.start(1)):
                try:
                    return json.loads(html[m.start(1):end])
                except json.JSONDecodeError:
                    continue
            try:
                return json.loads(m.group(1))
            except json.JSONDecodeError:
                continue
    return None


def _balanced_ends(s: str, start: int):
    depth = 0
    in_str = esc = False
    for i in range(start, len(s)):
        c = s[i]
        if in_str:
            if esc:
                esc = False
            elif c == "\\":
                esc = True
            elif c == '"':
                in_str = False
        else:
            if c == '"':
                in_str = True
            elif c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
                if depth == 0:
                    yield i + 1
                    return


def parse_video(html: str, video_id: str) -> Optional[dict]:
    player = extract_json_var(html, "ytInitialPlayerResponse") or {}
    data   = extract_json_var(html, "ytInitialData") or {}

    details      = player.get("videoDetails", {})
    title        = details.get("title")
    view_count   = None
    uploader_id  = details.get("channelId")
    uploader_name = details.get("author")

    if details.get("viewCount"):
        try:
            view_count = int(details["viewCount"])
        except (ValueError, TypeError):
            pass

    if not title:
        title = _find_video_title(data)
    if view_count is None:
        view_count = _find_view_count(data)
    if not uploader_id or not uploader_name:
        owner = _find_owner(data)
        if owner:
            uploader_id   = uploader_id   or owner.get("channel_id")
            uploader_name = uploader_name or owner.get("name")

    if not uploader_id:
        return None

    return {
        "video_id"   : video_id,
        "title"      : title or "",
        "view_count" : view_count,
        "uploader"   : {"channel_id": uploader_id, "name": uploader_name or ""},
        "collaborators": extract_collaborators(data, uploader_id),
        "related_ids": [v for v in extract_video_ids(data) if v != video_id],
    }


def _find_video_title(data: dict) -> Optional[str]:
    for node in walk(data):
        if isinstance(node, dict) and "videoPrimaryInfoRenderer" in node:
            return first_text(node["videoPrimaryInfoRenderer"].get("title"))
    return None


def _find_view_count(data: dict) -> Optional[int]:
    for node in walk(data):
        if isinstance(node, dict) and "videoViewCountRenderer" in node:
            txt = first_text(node["videoViewCountRenderer"].get("viewCount"))
            return parse_count(txt)
    return None


def _find_owner(data: dict) -> Optional[dict]:
    for node in walk(data):
        if isinstance(node, dict) and "videoOwnerRenderer" in node:
            r   = node["videoOwnerRenderer"]
            cid = _browse_id(r.get("navigationEndpoint") or r.get("title"))
            if cid:
                return {"channel_id": cid, "name": first_text(r.get("title"))}
    return None


def _browse_id(node: Any) -> Optional[str]:
    for n in walk(node):
        if isinstance(n, dict):
            be = n.get("browseEndpoint")
            if isinstance(be, dict):
                bid = be.get("browseId", "")
                if isinstance(bid, str) and bid.startswith("UC"):
                    return bid
    return None


def extract_collaborators(data: dict, uploader_id: str) -> list[dict]:
    """
    Three-strategy extraction of collaborating channel IDs from ytInitialData:
      A) Keys containing 'collab'
      B) videoSecondaryInfoRenderer (byline / channel cards)
      C) videoOwnerRenderer / channelRenderer nodes
    """
    found: dict[str, str] = {}

    def add(cid: Optional[str], name: Optional[str]) -> None:
        if cid and isinstance(cid, str) and cid.startswith("UC"):
            if cid not in found or (not found[cid] and name):
                found[cid] = name or found.get(cid, "") or ""

    # A — keys mentioning 'collab'
    for node in walk(data):
        if not isinstance(node, dict):
            continue
        for key, val in node.items():
            if "collab" in key.lower():
                for inner in walk(val):
                    if isinstance(inner, dict):
                        cid = _browse_id(inner)
                        if cid:
                            add(cid, first_text(inner.get("title"))
                                or first_text(inner.get("text")))

    # B — secondary info / byline
    for node in walk(data):
        if isinstance(node, dict) and "videoSecondaryInfoRenderer" in node:
            for inner in walk(node["videoSecondaryInfoRenderer"]):
                if isinstance(inner, dict):
                    be = inner.get("browseEndpoint")
                    if isinstance(be, dict):
                        bid = be.get("browseId", "")
                        if isinstance(bid, str) and bid.startswith("UC"):
                            add(bid, first_text(inner.get("text")) or None)

    # C — owner / channel renderers
    for node in walk(data):
        if isinstance(node, dict):
            for key in ("videoOwnerRenderer", "channelRenderer",
                        "gridChannelRenderer"):
                if key in node:
                    r   = node[key]
                    cid = r.get("channelId") or _browse_id(r)
                    add(cid, first_text(r.get("title")))

    found.pop(uploader_id, None)
    return [{"channel_id": cid, "name": name} for cid, name in found.items()]


def _viewmodel_text(node: Any) -> Optional[str]:
    for n in walk(node):
        if isinstance(n, dict):
            v = n.get("content")
            if isinstance(v, str) and v.strip():
                return v
    return None


def _name_from_html(html: str) -> Optional[str]:
    import html as _h
    m = re.search(r'<meta property="og:title" content="([^"]*)"', html)
    if m and m.group(1).strip():
        return _h.unescape(m.group(1)).strip()
    m = re.search(r"<title>(.*?)</title>", html, re.DOTALL)
    if m:
        t = re.sub(r"\s*-\s*YouTube\s*$", "", _h.unescape(m.group(1)).strip())
        return t or None
    return None


def _subs_from_html(html: str) -> Optional[int]:
    m = re.search(r"([\d.,]+[KMB]?)\s+subscribers", html)
    return parse_count(m.group(0)) if m else None


def parse_channel(html: str, channel_id: str) -> Optional[dict]:
    data  = extract_json_var(html, "ytInitialData") or {}
    name  = None
    subs  = None

    for node in walk(data):
        if isinstance(node, dict):
            if "c4TabbedHeaderRenderer" in node:
                h    = node["c4TabbedHeaderRenderer"]
                name = name or first_text(h.get("title"))
                subs = subs or parse_count(first_text(h.get("subscriberCountText")))
            if "pageHeaderRenderer" in node:
                title = node["pageHeaderRenderer"].get("title")
                name  = name or first_text(title) or _viewmodel_text(title)
            txt = first_text(node.get("subscriberCountText"))
            if txt and subs is None and "subscriber" in txt.lower():
                subs = parse_count(txt)

    if not name:
        name = data.get("metadata", {}).get("channelMetadataRenderer", {}).get("title")
    if not name:
        name = _name_from_html(html)
    if subs is None:
        subs = _subs_from_html(html)

    if not name and subs is None:
        return None
    return {"channel_id": channel_id, "name": name or "", "subscribers": subs}


def extract_video_ids(data: dict) -> list[str]:
    ids: list[str] = []
    seen: set[str] = set()
    for node in walk(data):
        if isinstance(node, dict):
            vid = node.get("videoId")
            if isinstance(vid, str) and len(vid) == 11 and vid not in seen:
                seen.add(vid)
                ids.append(vid)
    return ids


def extract_channel_ids(data: dict) -> list[str]:
    ids, seen = [], set()
    for node in walk(data):
        if isinstance(node, dict):
            be = node.get("browseEndpoint")
            if isinstance(be, dict):
                bid = be.get("browseId", "")
                if isinstance(bid, str) and bid.startswith("UC") and bid not in seen:
                    seen.add(bid)
                    ids.append(bid)
    return ids


# =============================================================================
# Database (Store)
# =============================================================================

class Store:
    """
    SQLite-backed store for channels, videos, collab links, and crawl queues.
    Also holds API-enrichment data in additional columns on the channels table
    (added via non-destructive ALTER TABLE migration so existing DBs work fine).
    """

    def __init__(self, path: str):
        self.conn = sqlite3.connect(path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        c = self.conn
        c.executescript("""
            CREATE TABLE IF NOT EXISTS channels (
                channel_id  TEXT PRIMARY KEY,
                name        TEXT,
                url         TEXT,
                subscribers INTEGER,
                scraped     INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS videos (
                video_id    TEXT PRIMARY KEY,
                title       TEXT,
                url         TEXT,
                view_count  INTEGER,
                uploader_id TEXT
            );
            CREATE TABLE IF NOT EXISTS video_collaborators (
                video_id   TEXT,
                channel_id TEXT,
                PRIMARY KEY (video_id, channel_id)
            );
            CREATE TABLE IF NOT EXISTS video_queue (
                video_id TEXT PRIMARY KEY,
                done     INTEGER DEFAULT 0,
                priority INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS channel_queue (
                channel_id TEXT PRIMARY KEY,
                done       INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS seed_queries (
                query TEXT PRIMARY KEY,
                used  INTEGER DEFAULT 0
            );
        """)
        # Non-destructive migration: add new columns if they don't exist yet
        _migrate_add_column(c, "video_queue", "priority INTEGER DEFAULT 0")
        for col in [
            "yt_country TEXT",
            "yt_topics TEXT",
            "yt_main_topic TEXT",
            "yt_topic_urls TEXT",
            "yt_desc TEXT",
            "yt_keywords TEXT",
            "yt_view_count TEXT",
            "yt_video_count TEXT",
            "yt_sub_count_api TEXT",
            "enriched INTEGER DEFAULT 0",
        ]:
            _migrate_add_column(c, "channels", col)
        c.commit()

    # ---- seed query pool --------------------------------------------------

    def init_seed_pool(self, queries: Iterable[str]) -> None:
        self.conn.executemany(
            "INSERT OR IGNORE INTO seed_queries (query) VALUES (?)",
            [(q,) for q in queries],
        )
        self.conn.commit()

    def next_seed_queries(self, limit: int) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT query FROM seed_queries WHERE used=0 LIMIT ?", (limit,)
        ).fetchall()]

    def mark_seed_used(self, query: str) -> None:
        self.conn.execute("UPDATE seed_queries SET used=1 WHERE query=?", (query,))

    # ---- channels ---------------------------------------------------------

    def upsert_channel(self, cid: str, name: Optional[str],
                       subs: Optional[int]) -> None:
        url = f"https://www.youtube.com/channel/{cid}"
        self.conn.execute(
            """INSERT INTO channels (channel_id, name, url, subscribers)
               VALUES (?,?,?,?)
               ON CONFLICT(channel_id) DO UPDATE SET
                 name=COALESCE(NULLIF(excluded.name,''), channels.name),
                 subscribers=COALESCE(excluded.subscribers, channels.subscribers)
            """,
            (cid, name, url, subs),
        )

    def save_enrichment(self, cid: str, data: dict) -> None:
        """Write YouTube API enrichment fields for one channel."""
        self.conn.execute(
            """UPDATE channels SET
                 yt_country=?, yt_topics=?, yt_main_topic=?, yt_topic_urls=?,
                 yt_desc=?, yt_keywords=?, yt_view_count=?, yt_video_count=?,
                 yt_sub_count_api=?, enriched=1
               WHERE channel_id=?""",
            (
                data.get("yt_country", ""),
                data.get("yt_topics", ""),
                data.get("yt_main_topic", ""),
                data.get("yt_topic_urls", ""),
                data.get("yt_desc", ""),
                data.get("yt_keywords", ""),
                data.get("yt_view_count", ""),
                data.get("yt_video_count", ""),
                data.get("yt_sub_count_api", ""),
                cid,
            ),
        )

    def mark_channel_scraped(self, cid: str) -> None:
        self.conn.execute("UPDATE channels SET scraped=1 WHERE channel_id=?", (cid,))

    def reset_missing_for_retry(self) -> int:
        cur = self.conn.execute(
            """UPDATE channels SET scraped=0 WHERE
                 name IS NULL OR name='' OR
                 (subscribers IS NULL
                  AND name NOT LIKE '%VEVO'
                  AND name NOT LIKE '%- Topic')"""
        )
        self.conn.commit()
        return cur.rowcount

    def channel_count(self) -> int:
        return self.conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]

    def uploader_video_count(self, cid: str) -> int:
        return self.conn.execute(
            "SELECT COUNT(*) FROM videos WHERE uploader_id=?", (cid,)
        ).fetchone()[0]

    def channels_needing_scrape(self) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT channel_id FROM channels WHERE scraped=0"
        ).fetchall()]

    def channels_needing_enrichment(self) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT channel_id FROM channels WHERE enriched=0 OR enriched IS NULL"
        ).fetchall()]

    # ---- videos -----------------------------------------------------------

    def save_video(self, v: dict) -> None:
        self.conn.execute(
            """INSERT OR REPLACE INTO videos
               (video_id, title, url, view_count, uploader_id)
               VALUES (?,?,?,?,?)""",
            (
                v["video_id"], v["title"],
                f"https://www.youtube.com/watch?v={v['video_id']}",
                v["view_count"], v["uploader"]["channel_id"],
            ),
        )
        for col in v["collaborators"]:
            self.conn.execute(
                "INSERT OR IGNORE INTO video_collaborators VALUES (?,?)",
                (v["video_id"], col["channel_id"]),
            )

    # ---- queues -----------------------------------------------------------

    def enqueue_videos(self, ids: Iterable[str], priority: int = 0) -> None:
        self.conn.executemany(
            """INSERT INTO video_queue (video_id, priority) VALUES (?,?)
               ON CONFLICT(video_id) DO UPDATE SET
                 priority=MAX(video_queue.priority, excluded.priority)""",
            [(i, priority) for i in ids],
        )

    def enqueue_channels(self, ids: Iterable[str]) -> None:
        self.conn.executemany(
            "INSERT OR IGNORE INTO channel_queue (channel_id) VALUES (?)",
            [(i,) for i in ids],
        )

    def next_videos(self, limit: int) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT video_id FROM video_queue WHERE done=0 "
            "ORDER BY priority DESC, rowid ASC LIMIT ?", (limit,)
        ).fetchall()]

    def next_channels(self, limit: int) -> list[str]:
        return [r[0] for r in self.conn.execute(
            "SELECT channel_id FROM channel_queue WHERE done=0 LIMIT ?", (limit,)
        ).fetchall()]

    def mark_video_done(self, vid: str) -> None:
        self.conn.execute("UPDATE video_queue SET done=1 WHERE video_id=?", (vid,))

    def mark_channel_done(self, cid: str) -> None:
        self.conn.execute(
            "UPDATE channel_queue SET done=1 WHERE channel_id=?", (cid,)
        )

    def commit(self) -> None:
        self.conn.commit()


def _migrate_add_column(conn: sqlite3.Connection, table: str, col_def: str) -> None:
    """ADD COLUMN if it doesn't exist yet (swallows the error if it does)."""
    try:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {col_def}")
    except sqlite3.OperationalError:
        pass  # column already exists


# =============================================================================
# Crawler  (Step 1)
# =============================================================================

class Crawler:
    def __init__(
        self,
        store: Store,
        fetcher: Fetcher,
        target: int,
        low_bias: bool = False,
        uploads_per_channel: int = UPLOADS_PER_CHANNEL,
        per_uploader_cap: Optional[int] = None,
        search_pages: Optional[int] = None,
    ):
        self.store   = store
        self.fetcher = fetcher
        self.target  = target
        self.low_bias = low_bias
        self.uploads_per_channel = uploads_per_channel
        self.per_uploader_cap    = per_uploader_cap
        self.seed_pool = NEUTRAL_MARKERS if low_bias else SEED_QUERIES
        self.search_pages = (
            (LOW_BIAS_SEARCH_PAGES if low_bias else SEARCH_PAGES)
            if search_pages is None else search_pages
        )

    # -- fetch helpers -------------------------------------------------------

    def _fetch_video(self, video_id: str) -> Optional[dict]:
        html = self.fetcher.get(f"https://www.youtube.com/watch?v={video_id}")
        return parse_video(html, video_id) if html else None

    def _fetch_channel(self, channel_id: str) -> Optional[dict]:
        html = self.fetcher.get(
            f"https://www.youtube.com/channel/{channel_id}/about"
        )
        return parse_channel(html, channel_id) if html else None

    def _fetch_channel_uploads(self, channel_id: str) -> list[str]:
        html = self.fetcher.get(
            f"https://www.youtube.com/channel/{channel_id}/videos"
        )
        if not html:
            return []
        data = extract_json_var(html, "ytInitialData") or {}
        return extract_video_ids(data)[: self.uploads_per_channel]

    def _fetch_trending(self) -> list[str]:
        ids: list[str] = []
        for url in TRENDING_URLS:
            html = self.fetcher.get(url)
            if html:
                ids += extract_video_ids(
                    extract_json_var(html, "ytInitialData") or {}
                )
        seen: set[str] = set()
        return [v for v in ids if not (v in seen or seen.add(v))]

    def _fetch_featured_channels(self, channel_id: str) -> list[str]:
        html = self.fetcher.get(
            f"https://www.youtube.com/channel/{channel_id}/channels"
        )
        if not html:
            return []
        data = extract_json_var(html, "ytInitialData") or {}
        return [c for c in extract_channel_ids(data) if c != channel_id]

    def _search_video_ids(self, query: str) -> list[str]:
        from urllib.parse import quote_plus
        html = self.fetcher.get(
            f"https://www.youtube.com/results?search_query={quote_plus(query)}"
        )
        if not html:
            return []
        data = extract_json_var(html, "ytInitialData") or {}
        ids  = extract_video_ids(data)
        if self.search_pages <= 1:
            return ids
        api_key    = _re1(r'"INNERTUBE_API_KEY":"([^"]+)"', html)
        client_ver = (
            _re1(r'"INNERTUBE_CONTEXT_CLIENT_VERSION":"([^"]+)"', html)
            or _re1(r'"clientVersion":"([^"]+)"', html)
        )
        token = find_continuation_token(data)
        seen  = set(ids)
        for _ in range(self.search_pages - 1):
            if not (api_key and client_ver and token):
                break
            resp = self.fetcher.post_json(
                f"https://www.youtube.com/youtubei/v1/search?key={api_key}",
                {
                    "context": {"client": {
                        "clientName": "WEB", "clientVersion": client_ver,
                        "hl": "en", "gl": "US",
                    }},
                    "continuation": token,
                },
            )
            if not resp:
                break
            for v in extract_video_ids(resp):
                if v not in seen:
                    seen.add(v)
                    ids.append(v)
            token = find_continuation_token(resp)
        return ids

    # -- channel resolution -------------------------------------------------

    def resolve_channel_id(self, token: str) -> Optional[str]:
        """Turn a channel ID / URL / @handle / plain name into a UC… ID."""
        token = token.strip()
        if not token or token.startswith("#"):
            return None
        if re.fullmatch(r"UC[\w-]{22}", token):
            return token
        m = re.search(r"/channel/(UC[\w-]{22})", token)
        if m:
            return m.group(1)
        if token.startswith("@") or token.startswith("http"):
            url  = token if token.startswith("http") else f"https://www.youtube.com/{token}"
            html = self.fetcher.get(url)
            cid  = self._channel_id_from_html(html) if html else None
            if cid:
                return cid
        return self._search_channel(token.lstrip("@"))

    def _search_channel(self, query: str) -> Optional[str]:
        from urllib.parse import quote_plus
        html = self.fetcher.get(
            f"https://www.youtube.com/results?search_query={quote_plus(query)}"
            "&sp=EgIQAg%253D%253D"
        )
        if not html:
            return None
        data = extract_json_var(html, "ytInitialData") or {}
        for node in walk(data):
            if isinstance(node, dict):
                r = node.get("channelRenderer") or node.get("gridChannelRenderer")
                if isinstance(r, dict):
                    cid = r.get("channelId")
                    if isinstance(cid, str) and cid.startswith("UC"):
                        return cid
        ids = extract_channel_ids(data)
        return ids[0] if ids else None

    @staticmethod
    def _channel_id_from_html(html: str) -> Optional[str]:
        return (
            _re1(r'"externalId":"(UC[\w-]{22})"', html)
            or _re1(r"/channel/(UC[\w-]{22})", html)
            or _re1(r'"channelId":"(UC[\w-]{22})"', html)
        )

    def seed_channels(self, tokens: list[str], uploads: int = 60) -> int:
        """Resolve each token and seed the crawl from its recent uploads."""
        n = 0
        for tok in tokens:
            tok = tok.strip()
            if not tok or tok.startswith("#"):
                continue
            cid = self.resolve_channel_id(tok)
            if not cid:
                log(f"  could not resolve: {tok!r}")
                continue
            html = self.fetcher.get(f"https://www.youtube.com/channel/{cid}/videos")
            vids = (
                extract_video_ids(extract_json_var(html, "ytInitialData") or {})[:uploads]
                if html else []
            )
            self.store.enqueue_videos(vids, priority=1)
            self.store.enqueue_channels([cid])
            self.store.commit()
            n += 1
            log(f"  seeded {tok!r} -> {cid} ({len(vids)} uploads)")
        return n

    def seed(self, batch: int = SEED_BATCH) -> int:
        """Pull unused seed queries, enqueue their video results at high priority."""
        queries = self.store.next_seed_queries(batch)
        if not queries:
            return 0
        log(f"Seeding from {len(queries)} fresh queries...")
        for q in queries:
            ids = self._search_video_ids(q)
            log(f"  '{q}' -> {len(ids)} videos")
            self.store.enqueue_videos(ids, priority=1)
            self.store.mark_seed_used(q)
        self.store.commit()
        return len(queries)

    # -- main crawl loop ----------------------------------------------------

    def run(self) -> None:
        self.store.init_seed_pool(self.seed_pool)
        if self.low_bias:
            trending = self._fetch_trending()
            log(f"Low-bias mode: seeded {len(trending)} trending videos.")
            self.store.enqueue_videos(trending, priority=1)
            self.store.commit()

        # On first run OR when resuming a stalled crawl, inject fresh seeds
        frontier_empty = (
            not self.store.next_videos(1) and not self.store.next_channels(1)
        )
        if frontier_empty or self.store.channel_count() > 0:
            self.seed()

        videos_since_new = 0
        last_count = self.store.channel_count()

        while self.store.channel_count() < self.target:
            progressed = False
            hungry     = videos_since_new >= STALE_LIMIT // 2

            # 1) Process a batch of videos (highest-priority first)
            for vid in self.store.next_videos(25):
                progressed  = True
                parsed      = self._fetch_video(vid)
                self.store.mark_video_done(vid)
                if not parsed:
                    continue

                up   = parsed["uploader"]
                cols = parsed["collaborators"]
                is_collab = bool(cols) and len(cols) <= MAX_COLLABORATORS

                if is_collab:
                    self.store.upsert_channel(up["channel_id"], up["name"], None)
                    self.store.save_video(parsed)
                    for col in cols:
                        self.store.upsert_channel(col["channel_id"], col["name"], None)
                        self.store.enqueue_channels([col["channel_id"]])
                    self.store.enqueue_channels([up["channel_id"]])
                self.store.commit()

                now = self.store.channel_count()
                if now > last_count:
                    last_count       = now
                    videos_since_new = 0
                else:
                    videos_since_new += 1

                if is_collab:
                    log(
                        f"  + {vid}: {len(cols)} collaborators "
                        f"(channels: {now}, stale: {videos_since_new})"
                    )
                elif cols:
                    log(
                        f"  ~ {vid}: {len(cols)} detected "
                        f"(>{MAX_COLLABORATORS} cap) — skipped"
                    )
                if now >= self.target:
                    break

            # 2) Re-seed if stagnant
            if videos_since_new >= STALE_LIMIT:
                used = self.seed()
                if used:
                    log(
                        f"Stagnant for {videos_since_new} videos — "
                        f"injected {used} fresh seeds."
                    )
                    videos_since_new = 0
                elif (
                    not self.store.next_videos(1)
                    and not self.store.next_channels(1)
                ):
                    log("Seed pool exhausted and frontier empty — stopping.")
                    break
                else:
                    videos_since_new = 0

            # 3) Expand channels into more candidate videos
            for cid in self.store.next_channels(10):
                progressed = True
                if (
                    self.per_uploader_cap is not None
                    and self.store.uploader_video_count(cid) >= self.per_uploader_cap
                ):
                    self.store.mark_channel_done(cid)
                    continue
                self.store.enqueue_videos(self._fetch_channel_uploads(cid))
                if hungry and not self.low_bias:
                    self.store.enqueue_channels(self._fetch_featured_channels(cid))
                self.store.mark_channel_done(cid)
                self.store.commit()

            if not progressed:
                if self.seed() == 0:
                    log("Nothing left to crawl — stopping.")
                    break

        log(f"Crawl finished with {self.store.channel_count()} channels.")
        self._enrich_channels_from_pages()

    def _enrich_channels_from_pages(self) -> None:
        """Fetch name + subscriber count for every un-scraped channel (web scrape)."""
        todo = self.store.channels_needing_scrape()
        log(f"Web-scrape enrichment: {len(todo)} channels...")
        for i, cid in enumerate(todo, 1):
            info = self._fetch_channel(cid)
            if info:
                self.store.upsert_channel(cid, info["name"], info["subscribers"])
            self.store.mark_channel_scraped(cid)
            if i % 25 == 0:
                self.store.commit()
                log(f"  {i}/{len(todo)} done")
        self.store.commit()


# =============================================================================
# API Enrichment  (Step 2)
# =============================================================================

def _parse_topic_url(url: str) -> str:
    """Convert a Wikipedia topic URL to a readable label."""
    return url.rstrip("/").split("/")[-1].replace("_", " ")


def enrich_channels_api(store: Store, api_key: str) -> None:
    """
    Query the YouTube Data API v3 for all un-enriched channels and write the
    results (topic, country, counts, etc.) back to the DB.

    Resumable: channels already marked enriched=1 are skipped.
    """
    to_enrich = store.channels_needing_enrichment()
    if not to_enrich:
        log("API enrichment: all channels already enriched — nothing to do.")
        return

    batches = [
        to_enrich[i: i + API_BATCH_SIZE]
        for i in range(0, len(to_enrich), API_BATCH_SIZE)
    ]
    log(f"API enrichment: {len(to_enrich)} channels in {len(batches)} batches...")
    errors = 0

    for i, batch in enumerate(batches):
        try:
            params = urllib.parse.urlencode({
                "part"      : "snippet,topicDetails,statistics,brandingSettings",
                "id"        : ",".join(batch),
                "key"       : api_key,
                "maxResults": API_BATCH_SIZE,
            })
            url = f"https://www.googleapis.com/youtube/v3/channels?{params}"
            req = urllib.request.Request(url, headers={"Accept": "application/json"})
            with urllib.request.urlopen(req, timeout=20) as response:
                data = json.loads(response.read().decode("utf-8"))

            if "error" in data:
                log(f"  API error batch {i+1}: {data['error'].get('message', '')}")
                errors += 1
                continue

            returned_ids: set[str] = set()
            for item in data.get("items", []):
                cid  = item["id"]
                returned_ids.add(cid)

                snippet    = item.get("snippet", {})
                stats      = item.get("statistics", {})
                topic      = item.get("topicDetails", {})
                brand      = item.get("brandingSettings", {}).get("channel", {})
                topics_raw = topic.get("topicCategories", [])
                topics     = [_parse_topic_url(t) for t in topics_raw]

                store.save_enrichment(cid, {
                    "yt_country"     : snippet.get("country", ""),
                    "yt_topics"      : " | ".join(topics),
                    "yt_main_topic"  : topics[0] if topics else "",
                    "yt_topic_urls"  : " | ".join(topics_raw),
                    "yt_desc"        : snippet.get("description", "")[:200].replace("\n", " "),
                    "yt_keywords"    : brand.get("keywords", "")[:200],
                    "yt_view_count"  : stats.get("viewCount", ""),
                    "yt_video_count" : stats.get("videoCount", ""),
                    "yt_sub_count_api": stats.get("subscriberCount", ""),
                })

            # Channels the API didn't return (deleted/private) — mark done with blanks
            for cid in batch:
                if cid not in returned_ids:
                    store.save_enrichment(cid, {})

            store.commit()
            log(
                f"  Batch {i+1:3d}/{len(batches)} — "
                f"{len(data.get('items', []))} channels returned"
            )
            time.sleep(API_SLEEP_SEC)

        except urllib.error.HTTPError as e:
            log(f"  HTTP error batch {i+1}: {e.code} {e.reason}")
            errors += 1
            time.sleep(1)
        except Exception as e:
            log(f"  Error batch {i+1}: {e}")
            errors += 1
            time.sleep(1)

    log(f"API enrichment complete. Errors: {errors}")


# =============================================================================
# Gephi Export  (Step 3)
# =============================================================================

def export_gephi(
    store: Store,
    out_dir: str = ".",
    max_collaborators: int = MAX_COLLABORATORS,
) -> tuple[str, str]:
    """
    Write two Gephi-ready CSV files to out_dir:

      gephi_nodes.csv
        Columns (always): Id, Label, subscribers, url, missing_subs, connected
        Extra columns (if step 2 was run): YT_Country, YT_Topics, YT_MainTopic,
          YT_ViewCount, YT_VideoCount, YT_SubCount, YT_Desc, YT_Keywords, YT_TopicURLs

      gephi_edges_undirected.csv
        Columns: Source, Target, Type, Weight, videos
        One row per channel pair; Weight = number of shared collab videos.

    Only channels that appear in at least one retained edge are included.

    Returns (nodes_path, edges_path).
    """
    os.makedirs(out_dir, exist_ok=True)
    conn = store.conn

    # ---- Build collab pairs -----------------------------------------------
    collabs: dict[str, list[str]] = defaultdict(list)
    for vid, cid in conn.execute(
        "SELECT video_id, channel_id FROM video_collaborators"
    ):
        collabs[vid].append(cid)
    uploader = dict(conn.execute("SELECT video_id, uploader_id FROM videos"))

    undirected_w: dict[tuple, int] = defaultdict(int)
    used_nodes: set[str] = set()
    kept = dropped = 0

    for vid, up in uploader.items():
        cols = collabs.get(vid, [])
        if not cols:
            continue
        if max_collaborators and len(cols) > max_collaborators:
            dropped += 1
            continue
        kept += 1
        participants = sorted(set([up, *cols]))
        for a, b in combinations(participants, 2):
            undirected_w[(a, b)] += 1
            used_nodes.update((a, b))

    log(
        f"Export: {kept} videos kept, {dropped} dropped "
        f"(>{max_collaborators} collaborators)"
    )

    # ---- Detect which columns exist on channels table ---------------------
    existing_cols = {row[1] for row in conn.execute("PRAGMA table_info(channels)")}
    has_enrichment = "yt_country" in existing_cols

    # ---- Nodes CSV --------------------------------------------------------
    base_fields = ["Id", "Label", "subscribers", "url", "missing_subs", "connected"]
    enrich_fields = (
        ["YT_Country", "YT_Topics", "YT_MainTopic",
         "YT_ViewCount", "YT_VideoCount", "YT_SubCount",
         "YT_Desc", "YT_Keywords", "YT_TopicURLs"]
        if has_enrichment else []
    )

    nodes_path = os.path.join(out_dir, "gephi_nodes.csv")
    n_nodes = 0
    with open(nodes_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=base_fields + enrich_fields)
        writer.writeheader()

        select_cols = (
            "channel_id, name, url, subscribers"
            + (", yt_country, yt_topics, yt_main_topic,"
               " yt_view_count, yt_video_count, yt_sub_count_api,"
               " yt_desc, yt_keywords, yt_topic_urls"
               if has_enrichment else "")
        )
        for row in conn.execute(f"SELECT {select_cols} FROM channels"):
            cid = row[0]
            if cid not in used_nodes:
                continue
            n_nodes += 1
            missing = row[3] is None
            record: dict = {
                "Id"         : cid,
                "Label"      : row[1] or cid,
                "subscribers": "" if missing else row[3],
                "url"        : row[2] or "",
                "missing_subs": "yes" if missing else "no",
                "connected"  : "yes",
            }
            if has_enrichment:
                record.update({
                    "YT_Country"   : row[4]  or "",
                    "YT_Topics"    : row[5]  or "",
                    "YT_MainTopic" : row[6]  or "",
                    "YT_ViewCount" : row[7]  or "",
                    "YT_VideoCount": row[8]  or "",
                    "YT_SubCount"  : row[9]  or "",
                    "YT_Desc"      : row[10] or "",
                    "YT_Keywords"  : row[11] or "",
                    "YT_TopicURLs" : row[12] or "",
                })
            writer.writerow(record)

    log(f"Nodes:  {n_nodes} channels -> {nodes_path}")

    # ---- Edges CSV --------------------------------------------------------
    edges_path = os.path.join(out_dir, "gephi_edges_undirected.csv")
    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["Source", "Target", "Type", "Weight", "videos"])
        for (s, t), wt in undirected_w.items():
            writer.writerow([s, t, "Undirected", wt, wt])

    log(f"Edges:  {len(undirected_w)} pairs -> {edges_path}")
    return nodes_path, edges_path


# =============================================================================
# Summary + debug helpers
# =============================================================================

def print_summary(store: Store, nodes_path: str, edges_path: str) -> None:
    conn = store.conn
    n_channels = conn.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
    n_videos   = conn.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    n_links    = conn.execute(
        "SELECT COUNT(*) FROM video_collaborators"
    ).fetchone()[0]
    n_enriched = conn.execute(
        "SELECT COUNT(*) FROM channels WHERE enriched=1"
    ).fetchone()[0]
    print("\n" + "=" * 56)
    print("  PIPELINE COMPLETE")
    print("=" * 56)
    print(f"  Collab videos saved      : {n_videos}")
    print(f"  Collaborator links       : {n_links}")
    print(f"  Unique channels          : {n_channels}")
    print(f"  Channels (API-enriched)  : {n_enriched}")
    print(f"  Nodes  -> {nodes_path}")
    print(f"  Edges  -> {edges_path}")
    print("=" * 56, flush=True)


def debug_video(fetcher: Fetcher, video_id: str) -> None:
    """Dump collab-related JSON structures for one video to debug_<id>.json."""
    html = fetcher.get(f"https://www.youtube.com/watch?v={video_id}")
    if not html:
        log("Could not fetch page.")
        return
    data = extract_json_var(html, "ytInitialData") or {}
    out  = {"keys_with_collab": [], "secondary_info": None}
    for node in walk(data):
        if isinstance(node, dict):
            for key in node:
                if "collab" in key.lower():
                    out["keys_with_collab"].append({key: node[key]})
            if "videoSecondaryInfoRenderer" in node:
                out["secondary_info"] = node["videoSecondaryInfoRenderer"]
    path = f"debug_{video_id}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, ensure_ascii=False)
    log(f"Wrote {path} — inspect 'keys_with_collab' / 'secondary_info'.")


def purge_noncollab(db_path: str) -> None:
    """Back up the DB then remove over-cap videos and orphaned channels."""
    import shutil
    backup = os.path.splitext(db_path)[0] + "_raw_backup.db"
    shutil.copy2(db_path, backup)
    log(f"Backed up -> {backup}")
    conn = sqlite3.connect(db_path)
    c    = conn.cursor()
    before_ch  = c.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
    before_vid = c.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    overcap = [v for (v,) in c.execute(
        "SELECT video_id FROM ("
        "  SELECT video_id, COUNT(*) n FROM video_collaborators GROUP BY video_id"
        ") WHERE n > ?",
        (MAX_COLLABORATORS,),
    )]
    c.executemany(
        "DELETE FROM video_collaborators WHERE video_id=?", [(v,) for v in overcap]
    )
    c.executemany("DELETE FROM videos WHERE video_id=?", [(v,) for v in overcap])
    c.execute(
        """DELETE FROM channels WHERE channel_id NOT IN (
               SELECT uploader_id FROM videos
               UNION SELECT channel_id FROM video_collaborators)"""
    )
    conn.commit()
    c.execute("VACUUM")
    after_ch  = c.execute("SELECT COUNT(*) FROM channels").fetchone()[0]
    after_vid = c.execute("SELECT COUNT(*) FROM videos").fetchone()[0]
    conn.close()
    log(f"Removed {len(overcap)} over-cap (>{MAX_COLLABORATORS}) videos.")
    log(f"Videos  : {before_vid} -> {after_vid}")
    log(f"Channels: {before_ch} -> {after_ch}")


# =============================================================================
# CLI
# =============================================================================

def main() -> None:
    ap = argparse.ArgumentParser(
        description="YouTube Collab SNA Pipeline: scrape → API-enrich → Gephi export",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Usage\n-----")[1] if "Usage\n-----" in __doc__ else "",
    )

    # Step control
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument(
        "--scrape-only", action="store_true",
        help="run step 1 (scrape) only",
    )
    grp.add_argument(
        "--enrich-only", action="store_true",
        help="run step 2 (API enrichment) only — requires --api-key",
    )
    grp.add_argument(
        "--export-only", action="store_true",
        help="run step 3 (Gephi export) only",
    )

    # Core options
    ap.add_argument("--api-key", default=None,
                    help="YouTube Data API v3 key (required for step 2)")
    ap.add_argument("--db", default=DEFAULT_DB,
                    help=f"SQLite database path (default: {DEFAULT_DB})")
    ap.add_argument("--out", default=".",
                    help="output directory for Gephi CSVs (default: current dir)")
    ap.add_argument("--target", type=int, default=DEFAULT_TARGET_CHANNELS,
                    help=f"stop after this many unique channels "
                         f"(default: {DEFAULT_TARGET_CHANNELS})")

    # Scraper tuning
    ap.add_argument("--min-delay", type=float, default=DEFAULT_MIN_DELAY)
    ap.add_argument("--max-delay", type=float, default=DEFAULT_MAX_DELAY)
    ap.add_argument("--proxy", default=None, help="optional http(s) proxy URL")
    ap.add_argument("--resume", action="store_true",
                    help="continue a previous crawl (implicit if --db already exists)")
    ap.add_argument("--low-bias", action="store_true",
                    help="reduce music/topic skew via topic-neutral seeds + upload cap")
    ap.add_argument("--uploads-per-channel", type=int, default=None,
                    help=f"uploads harvested per channel (default: {UPLOADS_PER_CHANNEL},"
                         f" or {LOW_BIAS_UPLOADS_PER_CHANNEL} in --low-bias mode)")
    ap.add_argument("--per-uploader-cap", type=int, default=None,
                    help="max recorded collab videos per channel before we stop "
                         f"expanding it (default: off, or {LOW_BIAS_PER_UPLOADER_CAP}"
                         " in --low-bias mode)")
    ap.add_argument("--search-pages", type=int, default=None,
                    help=f"search result pages to paginate per query "
                         f"(default: {SEARCH_PAGES}, or {LOW_BIAS_SEARCH_PAGES} in"
                         " --low-bias mode)")
    ap.add_argument("--seed-channels", metavar="FILE",
                    help="seed from a file of channel IDs/URLs/@handles, one per line")

    # Export tuning
    ap.add_argument("--max-collaborators", type=int, default=MAX_COLLABORATORS,
                    help=f"drop videos with more than N collaborators "
                         f"(default: {MAX_COLLABORATORS})")

    # Maintenance
    ap.add_argument("--retry-missing", action="store_true",
                    help="re-scrape channels missing a name or subscriber count")
    ap.add_argument("--purge-noncollab", action="store_true",
                    help="back up DB then delete over-cap videos + orphaned channels")
    ap.add_argument("--debug-video", metavar="VIDEO_ID",
                    help="dump one video's collab-related JSON and exit")

    args = ap.parse_args()

    fetcher = Fetcher(args.min_delay, args.max_delay, args.proxy)

    if args.debug_video:
        debug_video(fetcher, args.debug_video)
        return

    if args.purge_noncollab:
        purge_noncollab(args.db)
        store = Store(args.db)
        nodes_path, edges_path = export_gephi(
            store, args.out, args.max_collaborators
        )
        print_summary(store, nodes_path, edges_path)
        return

    store = Store(args.db)

    # Determine which steps run
    only_one  = args.scrape_only or args.enrich_only or args.export_only
    run_scrape = args.scrape_only or not only_one
    run_enrich = (args.enrich_only or not only_one) and bool(args.api_key)
    run_export = args.export_only or not only_one

    if args.enrich_only and not args.api_key:
        ap.error("--enrich-only requires --api-key")
    if not args.api_key and not only_one:
        log("No --api-key provided — skipping step 2 (API enrichment).")

    # ------------------------------------------------------------------
    # Step 1: Scrape
    # ------------------------------------------------------------------
    if run_scrape:
        log("\n=== Step 1: Scrape ===")

        if args.retry_missing:
            n = store.reset_missing_for_retry()
            log(f"Reset {n} channels for retry.")

        uploads = args.uploads_per_channel
        cap     = args.per_uploader_cap
        if args.low_bias:
            if uploads is None:
                uploads = LOW_BIAS_UPLOADS_PER_CHANNEL
            if cap is None:
                cap = LOW_BIAS_PER_UPLOADER_CAP
        if uploads is None:
            uploads = UPLOADS_PER_CHANNEL

        crawler = Crawler(
            store, fetcher, args.target,
            low_bias=args.low_bias,
            uploads_per_channel=uploads,
            per_uploader_cap=cap,
            search_pages=args.search_pages,
        )

        if args.seed_channels:
            with open(args.seed_channels, encoding="utf-8") as f:
                tokens = f.read().splitlines()
            log(f"Seeding from {args.seed_channels}...")
            got = crawler.seed_channels(tokens)
            log(f"Resolved and seeded {got} channels.")
            if store.channel_count() >= args.target:
                log(
                    f"NOTE: --target ({args.target}) <= current channels "
                    f"({store.channel_count()}); raise --target so the crawl runs."
                )

        try:
            crawler.run()
        except KeyboardInterrupt:
            log("Interrupted — progress saved. Re-run with --resume to continue.")
        finally:
            store.commit()

    # ------------------------------------------------------------------
    # Step 2: API Enrichment
    # ------------------------------------------------------------------
    if run_enrich:
        log("\n=== Step 2: API Enrichment ===")
        try:
            enrich_channels_api(store, args.api_key)
        except KeyboardInterrupt:
            log("Enrichment interrupted — progress saved. Re-run to continue.")
        finally:
            store.commit()

    # ------------------------------------------------------------------
    # Step 3: Gephi Export
    # ------------------------------------------------------------------
    if run_export:
        log("\n=== Step 3: Gephi Export ===")
        nodes_path, edges_path = export_gephi(
            store, args.out, args.max_collaborators
        )
        print_summary(store, nodes_path, edges_path)


if __name__ == "__main__":
    main()
