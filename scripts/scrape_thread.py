import requests
import pandas as pd
import time
import os
from collections import deque

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
}

def safe_get(url, params=None, retries=6):
    for attempt in range(retries):
        try:
            resp = requests.get(url, headers=HEADERS, params=params, timeout=15)
            if resp.status_code == 200 and resp.text.strip():
                return resp.json()
            elif resp.status_code in (429, 403):
                wait = 2 ** attempt
                print(f"  HTTP {resp.status_code} — sleeping {wait}s (attempt {attempt+1}/{retries})")
                time.sleep(wait)
            else:
                print(f"  HTTP {resp.status_code}, skipping")
                return None
        except Exception as e:
            print(f"  Request error: {e}")
            time.sleep(3)
    print("  Max retries hit, skipping batch")
    return None

def parse_comment_tree(children, all_comments, more_queue, seen_ids):
    for item in children:
        kind = item.get("kind")
        data = item.get("data", {})
        if kind == "t1":
            cid = str(data.get("id"))
            if cid not in seen_ids:
                seen_ids.add(cid)
                all_comments.append({
                    "comment_id":  cid,
                    "author":      data.get("author"),
                    "body":        data.get("body"),
                    "score":       data.get("score"),
                    "created_utc": data.get("created_utc"),
                    "depth":       data.get("depth"),
                    "parent_id":   data.get("parent_id"),
                })
            replies = data.get("replies")
            if isinstance(replies, dict):
                parse_comment_tree(replies["data"]["children"], all_comments, more_queue, seen_ids)
        elif kind == "more":
            ids = data.get("children", [])
            if ids:
                more_queue.append(ids)

def fetch_morechildren(chunk, link_fullname):
    result = safe_get("https://www.reddit.com/api/morechildren.json", params={
        "link_id":        link_fullname,
        "children":       ",".join(chunk),
        "api_type":       "json",
        "limit_children": "false",
    })
    if result:
        return result.get("json", {}).get("data", {}).get("things", [])
    return []

def scrape_thread(post_id, label):
    """
    Scrape a Reddit thread by post ID with checkpointing.
    label: short name for output files e.g. 'bills_broncos_p1'
    """
    checkpoint_file = f"checkpoint_{label}.csv"
    base_url = f"https://www.reddit.com/comments/{post_id}.json"

    # ── Load checkpoint if exists ──────────────────────────────────────
    all_comments = []
    seen_ids = set()
    if os.path.exists(checkpoint_file):
        existing = pd.read_csv(checkpoint_file)
        all_comments = existing.to_dict("records")
        seen_ids = set(existing["comment_id"].astype(str))
        print(f"[{label}] Resuming from checkpoint: {len(all_comments)} comments")
    else:
        print(f"[{label}] No checkpoint found, starting fresh")

    # ── Initial fetch ──────────────────────────────────────────────────
    print(f"[{label}] Fetching initial comment tree...")
    time.sleep(2)
    data = safe_get(base_url, params={"limit": 500, "depth": 10})

    if not data:
        print(f"[{label}] Failed initial fetch. Try again in a few minutes.")
        return None

    link_fullname = data[0]["data"]["children"][0]["data"]["name"]
    print(f"[{label}] Post fullname: {link_fullname}")

    more_queue = deque()
    parse_comment_tree(data[1]["data"]["children"], all_comments, more_queue, seen_ids)
    print(f"[{label}] Initial pass: {len(all_comments)} comments, {len(more_queue)} batches queued")

    # ── Expand ────────────────────────────────────────────────────────
    batch_count = 0
    while more_queue:
        batch = more_queue.popleft()
        for i in range(0, len(batch), 100):
            chunk = batch[i:i+100]
            things = fetch_morechildren(chunk, link_fullname)
            parse_comment_tree(things, all_comments, more_queue, seen_ids)
            batch_count += 1

            if batch_count % 10 == 0:
                print(f"  [{label}] {batch_count} batches | {len(all_comments)} comments")

            # Save checkpoint every 50 batches
            if batch_count % 50 == 0:
                pd.DataFrame(all_comments).to_csv(checkpoint_file, index=False)
                print(f"  [{label}] >>> Checkpoint saved ({len(all_comments)} comments)")

            time.sleep(2)

    # ── Final save ────────────────────────────────────────────────────
    df = pd.DataFrame(all_comments).drop_duplicates(subset="comment_id")
    df.to_csv(f"raw_{label}.csv", index=False)
    print(f"[{label}] Done! {len(df)} total comments saved to raw_{label}.csv")

    # Clean up checkpoint now that we're done
    if os.path.exists(checkpoint_file):
        os.remove(checkpoint_file)
        print(f"[{label}] Checkpoint file removed")

    return df


# ── Run all four threads sequentially ─────────────────────────────────
# Comment out any you've already done
threads = [
    ("1qfrol6", "bills_broncos_p2"),   # Bills/Broncos 2nd half (your original)
    ("1qfnyfb", "bills_broncos_p1"),   # Bills/Broncos 1st half
    ("1nt2qh6", "packers_cowboys"),    # Packers/Cowboys
    ("1p931uu", "bears_eagles"),       # Bears/Eagles
]

for post_id, label in threads:
    print(f"\n{'='*60}")
    print(f"Starting: {label}")
    print(f"{'='*60}")
    scrape_thread(post_id, label)
    print(f"\nSleeping 15 minutes before next thread to avoid IP block...")
    time.sleep(1200)

print("\nAll threads scraped!")