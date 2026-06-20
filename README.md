# YouTube Collab SNA Pipeline

A self-contained Python pipeline for building a **Social Network Analysis (SNA)** dataset from YouTube's Collaborators feature — the mechanism that lets a channel formally credit up to five collaborating channels on a single video.

The pipeline runs three sequential steps and produces two Gephi-ready CSV files:

| Output file | Contents |
|---|---|
| `gephi_nodes.csv` | One row per channel, with subscriber count, URL, and optional API-enriched metadata (topics, country, view counts, etc.) |
| `gephi_edges_undirected.csv` | One row per collaborating channel pair; `Weight` = number of shared collab videos |

---

## How It Works

### Step 1 — Scrape (no API key required)

Uses a **snowball / BFS crawl** over public YouTube watch and channel pages:

1. Seeds from a pool of ~250 diverse collab-heavy search queries (or trending feeds in `--low-bias` mode).
2. For each candidate video, downloads the watch page and parses the embedded `ytInitialData` JSON to detect the Collaborators panel (up to 5 credited channels).
3. Any video with ≥ 1 collaborator is saved; all channels on it are queued for further expansion.
4. Repeat until `--target` unique channels are collected or the seed pool is exhausted.

**Stagnation control:** if the crawl goes 60 videos without finding a new channel (stuck in a dense cluster), it injects fresh high-priority seed queries to break out.

Everything is stored in a **resumable SQLite database** (`--db`, default `collab_data.db`). Interrupt at any time and re-run to pick up where you left off.

> **Why scraping and not the YouTube Data API?**
> The Collaborators list is not exposed by the YouTube Data API — it only exists in the rendered watch page inside the `ytInitialData` JSON blob. No API key is needed for this step.

### Step 2 — API Enrichment (optional, requires `--api-key`)

Queries the **YouTube Data API v3** (`channels.list`) for every channel discovered in Step 1, adding:

- Topic categories (e.g. "Music", "Gaming", "Sports")
- Country of origin
- Channel description & keyword tags
- Exact view count, video count, and subscriber count

Results are written back into the SQLite DB. The step is **resumable** — channels already enriched are skipped on re-run. The free API tier gives 10,000 units/day; enriching 5,000 channels uses ~100 units.

### Step 3 — Gephi Export

Reads the (optionally enriched) DB and writes:

- **`gephi_nodes.csv`** — all channels that appear in at least one retained edge, with all enrichment columns included if Step 2 was run.
- **`gephi_edges_undirected.csv`** — one row per channel pair, `Type=Undirected`, `Weight` = shared collab video count.

Videos with more than `--max-collaborators` collaborators (default 5, YouTube's feature cap) are dropped to avoid over-capture from compilation or edit videos.

---

## Setup

```bash
pip install requests
```

`requests` is the only third-party dependency (used by the web scraper). The API enrichment and export steps use only the Python standard library.

---

## Usage

```bash
# Full pipeline — scrape, enrich with API data, export to Gephi
python pipeline.py --api-key YOUR_YT_DATA_API_KEY

# Scrape only (no API key needed)
python pipeline.py --scrape-only

# Enrich already-scraped data
python pipeline.py --enrich-only --api-key YOUR_KEY

# Re-export Gephi CSVs from an existing DB (no network calls)
python pipeline.py --export-only

# Resume an interrupted run
python pipeline.py --resume --api-key YOUR_KEY

# Smaller test run (500 channels)
python pipeline.py --target 500 --api-key YOUR_KEY

# Reduce music/topic bias
python pipeline.py --low-bias --api-key YOUR_KEY

# Seed from a specific list of channels
python pipeline.py --seed-channels top_channels.txt --api-key YOUR_KEY

# Debug collaborator parsing on one video
python pipeline.py --debug-video VIDEO_ID
```

### All Options

| Flag | Default | Description |
|---|---|---|
| `--api-key KEY` | — | YouTube Data API v3 key (Step 2 only) |
| `--db PATH` | `collab_data.db` | SQLite database path |
| `--out DIR` | `.` | Output directory for Gephi CSVs |
| `--target N` | `3000` | Stop after N unique collab-feature channels |
| `--low-bias` | off | Topic-neutral seeds; fewer uploads per channel; per-channel cap |
| `--uploads-per-channel N` | `30` | Recent uploads harvested per channel (10 in `--low-bias`) |
| `--per-uploader-cap N` | off | Stop expanding a channel once N of its collab videos are recorded (20 in `--low-bias`) |
| `--max-collaborators N` | `5` | Drop videos with more than N collaborators |
| `--seed-channels FILE` | — | Seed from a file of channel IDs / URLs / @handles (one per line) |
| `--min-delay` / `--max-delay` | `1.5` / `3.5` | Seconds between HTTP requests |
| `--proxy URL` | — | Optional HTTP/S proxy |
| `--retry-missing` | — | Re-scrape channels missing a name or subscriber count |
| `--purge-noncollab` | — | Back up DB then remove over-cap videos and orphaned channels |
| `--scrape-only` | — | Run Step 1 only |
| `--enrich-only` | — | Run Step 2 only (requires `--api-key`) |
| `--export-only` | — | Run Step 3 only |
| `--debug-video ID` | — | Dump one video's collab-related JSON and exit |

---

## Output Format

### `gephi_nodes.csv`

| Column | Description |
|---|---|
| `Id` | YouTube channel ID (`UC…`) |
| `Label` | Channel display name |
| `subscribers` | Subscriber count scraped from the channel page |
| `url` | `https://www.youtube.com/channel/<id>` |
| `missing_subs` | `yes` if subscriber count couldn't be scraped |
| `connected` | Always `yes` (isolated nodes are excluded by default) |
| `YT_Country` ★ | Country code (e.g. `US`, `GB`) |
| `YT_Topics` ★ | Pipe-separated topic labels (e.g. `Music \| Gaming`) |
| `YT_MainTopic` ★ | First/primary topic |
| `YT_TopicURLs` ★ | Raw Wikipedia topic URLs from the API |
| `YT_ViewCount` ★ | Total channel view count |
| `YT_VideoCount` ★ | Total video count |
| `YT_SubCount` ★ | Exact subscriber count from the API |
| `YT_Desc` ★ | First 200 chars of channel description |
| `YT_Keywords` ★ | Channel keyword tags (first 200 chars) |

★ Present only if Step 2 (API enrichment) was run.

### `gephi_edges_undirected.csv`

| Column | Description |
|---|---|
| `Source` | Channel ID of one endpoint |
| `Target` | Channel ID of the other endpoint |
| `Type` | Always `Undirected` |
| `Weight` | Number of collab videos the pair shares |
| `videos` | Same as Weight (convenience column for Gephi import) |

---

## Getting a YouTube Data API Key

1. Go to [Google Cloud Console](https://console.cloud.google.com/).
2. Create or select a project → **APIs & Services** → **Enable APIs** → enable **YouTube Data API v3**.
3. **Credentials** → **Create Credentials** → **API key**.
4. Copy the key and pass it as `--api-key`.

The free tier gives 10,000 units/day. `channels.list` costs 1 unit per batch of 50 channels, so enriching 5,000 channels costs ~100 units total.

---

## Notes on Sampling Bias

The snowball crawl over-represents channels densely connected to the initial seeds. `--low-bias` mode mitigates this by:

- Seeding from topic-neutral Trending feeds and generic collaboration keywords instead of genre-specific music queries.
- Harvesting fewer uploads per channel (`--uploads-per-channel 10` vs. 30).
- Capping amplification so no single dense hub dominates (`--per-uploader-cap 20`).

Neither approach achieves uniform random sampling — no "random channel" endpoint exists — but `--low-bias` produces a substantially flatter topic distribution. Run it into a **fresh database**; existing data collected in default mode is already skewed and can't be corrected after the fact.

---

## Project Context

Built for a Social Network Analysis project studying YouTube's Collab feature as a formal mechanism for inter-channel collaboration. The resulting network can be loaded into [Gephi](https://gephi.org/) for community detection, centrality analysis, and cross-topic collaboration pattern analysis.
