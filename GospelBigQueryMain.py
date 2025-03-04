import os
import json
import time

# Google APIs
from google.oauth2 import service_account
from googleapiclient.discovery import build

# SerpAPI
from serpapi import GoogleSearch

# OpenAI
import openai

# For .env loading (optional)
from dotenv import load_dotenv
load_dotenv()

SERP_API_KEY = os.getenv("SERP_API_KEY") or "YOUR_SERP_API_KEY"
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY") or "YOUR_OPENAI_API_KEY"

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
FOLDER_ID = "1JTDdz7v3ohW1CC8AzUBd5xADI9dOAycu"
CACHE_SPREADSHEET_ID = "1OoBpd-zcv-MZwG6LOoaPPkOpZi6fKNHsk_VIQxna634"
TARGET_TAB = "Videos - Raw Data"
CHUNK_SIZE = 75

# Column indices in the row data (0-based)
COL_CHANNEL_NAME = 1   # B
COL_YOUTUBE_URL  = 14  # O
COL_DESCRIPTION  = 20  # U
COL_CATEGORY     = 21  # V

PROGRESS_JSON = "progress_state.json"
SERVICE_ACCOUNT_FILE = "/Users/keithjohnson/Desktop/GospelStatsBrandReports/rich-stratum-367618-60ffa4f70aea.json"

CATEGORIES = [
    "Video Games", "Entertainment", "Education", "Sports", "Technology",
    "Music", "Podcast", "Politics", "Travel", "Automotive",
    "Movies", "Humor", "Food", "Science", "Fashion",
    "Real Estate", "Photography", "Home Improvement", "Finance",
    "Uncategorized"
]

client = openai  # or your custom wrapper

# -------------------------------------------------------------------
# RATE-LIMIT VARIABLES
# -------------------------------------------------------------------
READ_COUNT = 0
START_MINUTE = time.time()
MAX_READS_PER_MINUTE = 60
THROTTLE_THRESHOLD = 55

def main():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/drive",
                "https://www.googleapis.com/auth/spreadsheets"]
    )
    drive_service = build("drive", "v3", credentials=creds)
    sheets_service = build("sheets", "v4", credentials=creds)

    # Load or create progress JSON
    if os.path.exists(PROGRESS_JSON):
        with open(PROGRESS_JSON, "r") as f:
            progress_state = json.load(f)
    else:
        progress_state = {}

    # Load initial channel cache (A:D)
    channel_cache = load_channel_cache(sheets_service, CACHE_SPREADSHEET_ID)

    # 1) List all spreadsheets in folder (one or more read calls with pageToken).
    check_rate_limit()
    resp = drive_service.files().list(
        q=f"'{FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.spreadsheet'",
        spaces='drive',
        fields='nextPageToken, files(id, name)'
    ).execute()
    increment_read_count()
    spreadsheets = resp.get("files", [])
    page_token = resp.get("nextPageToken")

    while page_token:
        check_rate_limit()
        resp = drive_service.files().list(
            q=f"'{FOLDER_ID}' in parents and mimeType='application/vnd.google-apps.spreadsheet'",
            spaces='drive',
            fields='nextPageToken, files(id, name)',
            pageToken=page_token
        ).execute()
        increment_read_count()
        spreadsheets.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")

    print(f"Found {len(spreadsheets)} spreadsheet(s) in folder {FOLDER_ID}.")

    for f in spreadsheets:
        ss_id = f["id"]
        ss_name = f["name"]
        print(f"\n=== Processing: {ss_name} ({ss_id}) ===")

        # 2) Read metadata for this spreadsheet (ONE read request).
        check_rate_limit()
        ss_meta = sheets_service.spreadsheets().get(spreadsheetId=ss_id).execute()
        increment_read_count()

        sheet_titles = [s['properties']['title'] for s in ss_meta['sheets']]
        if TARGET_TAB not in sheet_titles:
            print(f" - No sheet named '{TARGET_TAB}'. Skipping.")
            continue

        tab_props = next((s['properties'] for s in ss_meta['sheets']
                          if s['properties']['title'] == TARGET_TAB), None)
        if not tab_props:
            print(f" - Unable to load properties for '{TARGET_TAB}'. Skipping.")
            continue

        total_rows = tab_props.get('gridProperties', {}).get('rowCount', 0)
        if total_rows <= 1:
            print(" - Sheet is empty or only headers. Skipping.")
            continue

        # 3) Expand columns & ensure headers using the cached ss_meta
        expand_sheet_columns_if_needed(
            sheets_service, ss_id, TARGET_TAB, min_columns=22, ss_meta=ss_meta
        )
        ensure_headers_uv(sheets_service, ss_id, TARGET_TAB)  # still does one read for U1:V1

        last_key = f"LAST_ROW_{ss_id}"
        last_processed = progress_state.get(last_key, 1)

        while last_processed < total_rows:
            start_row = last_processed + 1
            end_row = min(start_row + CHUNK_SIZE - 1, total_rows)
            print(f" - Processing rows {start_row} to {end_row} of {total_rows} in {ss_name}.")

            # 4) Read chunk: that’s a read request
            a1_range = f"'{TARGET_TAB}'!A{start_row}:V{end_row}"
            check_rate_limit()
            result = sheets_service.spreadsheets().values().get(
                spreadsheetId=ss_id,
                range=a1_range
            ).execute()
            increment_read_count()

            rows = result.get("values", [])
            if not rows:
                print("   - No data returned for that range.")
                last_processed = end_row
                progress_state[last_key] = last_processed
                save_progress(progress_state)
                continue

            # Pad short rows
            for i, rdata in enumerate(rows):
                if len(rdata) < 22:
                    rows[i] = rdata + [""] * (22 - len(rdata))

            updated = []
            channels_to_classify = []
            channel_rows_map = {}
            rows_to_append_in_cache = []

            for i, row_data in enumerate(rows):
                channel_name = row_data[COL_CHANNEL_NAME]  
                channel_url  = row_data[COL_YOUTUBE_URL]   
                current_desc = row_data[COL_DESCRIPTION]   
                current_cat  = row_data[COL_CATEGORY]      

                if not channel_name or (current_desc and current_cat):
                    updated.append(row_data)
                    continue

                if channel_name in channel_cache:
                    row_data[COL_DESCRIPTION] = channel_cache[channel_name]["Description"]
                    row_data[COL_CATEGORY]    = channel_cache[channel_name]["Category"]
                else:
                    channels_to_classify.append(channel_name)
                    channel_rows_map.setdefault(channel_name, {"rows": [], "url": None})
                    channel_rows_map[channel_name]["rows"].append(i)
                    channel_rows_map[channel_name]["url"] = channel_url or ""

                updated.append(row_data)

            if channels_to_classify:
                classification_results = classify_channels_with_openai(channels_to_classify, batch_size=10)

                for res in classification_results:
                    ch = res["Channel"]
                    desc = res["Description"]
                    cat = res["Category"]
                    mapped_info = channel_rows_map[ch]
                    youtube_url = mapped_info["url"]

                    channel_cache[ch] = {
                        "Description": desc,
                        "Category": cat,
                        "YouTubeURL": youtube_url
                    }

                    rows_to_append_in_cache.append([ch, desc, cat, youtube_url])

                    for row_idx in mapped_info["rows"]:
                        updated[row_idx][COL_DESCRIPTION] = desc
                        updated[row_idx][COL_CATEGORY]    = cat

            # Single append if new channels (writes)
            if rows_to_append_in_cache:
                append_many_channels_to_cache(sheets_service, CACHE_SPREADSHEET_ID, rows_to_append_in_cache)

            # Write updated chunk back (writes)
            body = {"values": updated}
            sheets_service.spreadsheets().values().update(
                spreadsheetId=ss_id,
                range=a1_range,
                valueInputOption="RAW",
                body=body
            ).execute()
            print(f"   - Wrote {len(updated)} rows back for {ss_name}.")

            last_processed = end_row
            progress_state[last_key] = last_processed
            save_progress(progress_state)

            # 5) Reload the cache if you want fresh data for the next chunk
            channel_cache = load_channel_cache(sheets_service, CACHE_SPREADSHEET_ID)

            if last_processed >= total_rows:
                print("   - Finished all rows in this sheet.")
                break

    print("\nAll done for this run.")


# -------------------------------------------------------------------
# RATE-LIMIT HELPERS
# -------------------------------------------------------------------
def check_rate_limit():
    """Check if we've reached THROTTLE_THRESHOLD in this minute.
       If so, sleep until 60s is up from START_MINUTE, then reset."""
    global READ_COUNT, START_MINUTE
    if READ_COUNT >= THROTTLE_THRESHOLD:
        elapsed = time.time() - START_MINUTE
        if elapsed < 60:
            to_sleep = 60 - elapsed
            print(f"Hit {READ_COUNT} reads; sleeping {to_sleep:.1f}s to avoid 429 errors...")
            time.sleep(to_sleep)
        READ_COUNT = 0
        START_MINUTE = time.time()

def increment_read_count():
    """Increment READ_COUNT after a read call; reset if a minute has passed."""
    global READ_COUNT, START_MINUTE
    READ_COUNT += 1
    if time.time() - START_MINUTE >= 60:
        READ_COUNT = 0
        START_MINUTE = time.time()

# -------------------------------------------------------------------
# EXPAND COLUMNS BUT USE ss_meta
# -------------------------------------------------------------------
def expand_sheet_columns_if_needed(sheets_service, spreadsheet_id, sheet_name, min_columns=22, ss_meta=None):
    """
    If we already have ss_meta from the main code, use it. 
    No extra read. 
    """
    if not ss_meta:
        # fallback: do a read if none was provided
        check_rate_limit()
        ss_meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        increment_read_count()

    sheet_id = None
    current_col_count = None

    for s in ss_meta["sheets"]:
        props = s["properties"]
        if props["title"] == sheet_name:
            sheet_id = props["sheetId"]
            current_col_count = props.get("gridProperties", {}).get("columnCount", 0)
            break

    if sheet_id is None:
        print(f"Sheet '{sheet_name}' not found, cannot expand.")
        return

    if current_col_count < min_columns:
        print(f" - Expanding '{sheet_name}' from {current_col_count} columns to {min_columns}.")
        requests = [{
            "updateSheetProperties": {
                "properties": {
                    "sheetId": sheet_id,
                    "gridProperties": {
                        "columnCount": min_columns
                    }
                },
                "fields": "gridProperties.columnCount"
            }
        }]
        # This is a write call, no read increment
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests}
        ).execute()
    else:
        print(f" - '{sheet_name}' already has {current_col_count} columns (>= {min_columns}).")

def ensure_headers_uv(sheets_service, spreadsheet_id, sheet_name):
    """
    We don't re-fetch ss_meta here. We only read the actual cells U1:V1 to see if they're correct.
    That is 1 read call for the range.
    """
    header_range = f"'{sheet_name}'!U1:V1"
    check_rate_limit()
    result = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=header_range
    ).execute()
    increment_read_count()

    values = result.get("values", [])

    if not values or not values[0]:
        existing_u, existing_v = "", ""
    else:
        row_data = values[0]
        existing_u = row_data[0] if len(row_data) > 0 else ""
        existing_v = row_data[1] if len(row_data) > 1 else ""

    update_needed = False
    if existing_u.strip().lower() != "description":
        existing_u = "Description"
        update_needed = True
    if existing_v.strip().lower() != "category":
        existing_v = "Category"
        update_needed = True

    if update_needed:
        print(" - Updating headers in columns U/V.")
        body = {"values": [[existing_u, existing_v]]}
        # write (no read increment)
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=header_range,
            valueInputOption="RAW",
            body=body
        ).execute()

def fetch_channel_info(channel_name):
    if not SERP_API_KEY or SERP_API_KEY == "YOUR_SERP_API_KEY":
        print("[WARNING] No valid SERP_API_KEY found.")
        return ""

    params = {
        "q": f"{channel_name} YouTube channel",
        "engine": "google",
        "location": "United States",
        "hl": "en",
        "gl": "us",
        "api_key": SERP_API_KEY,
        "num": 5,
    }

    search = GoogleSearch(params)
    results = search.get_dict()
    snippet = ""

    kg = results.get("knowledge_graph")
    if kg:
        desc = kg.get("description")
        if desc:
            snippet += f"Knowledge Graph Description: {desc}\n"

    if "organic_results" in results:
        for r in results["organic_results"]:
            title = r.get("title", "")
            snippet_text = r.get("snippet", "")
            link = r.get("link", "")
            if "youtube.com" in link.lower() or "channel" in title.lower():
                snippet += f"* {title}: {snippet_text}\n"

    return snippet.strip()

def classify_channels_with_openai(channel_list, batch_size=10):
    results = []
    for i in range(0, len(channel_list), batch_size):
        batch = channel_list[i : i + batch_size]
        for channel_name in batch:
            serp_snippet = fetch_channel_info(channel_name)

            user_content = f"""
We have external info about this YouTube channel:

{serp_snippet or "(No snippet found)"}

Your task:
1) Provide a short "Description" of the channel's typical content.
2) Pick exactly one "Category" from this list:
{', '.join(CATEGORIES)}

Return ONLY valid JSON with this structure:
{{
  "Description": "...",
  "Category": "..."
}}

If uncertain, use "Uncategorized" for "Category".
Channel: {channel_name}

IMPORTANT:
- No extra text, disclaimers, or code fences.
- Output must be valid JSON, no markdown or additional text.
"""
            try:
                completion = client.chat.completions.create(
                    model="gpt-4o-mini",
                    messages=[
                        {
                            "role": "system",
                            "content": "You are a helpful assistant. Output only valid JSON per user request."
                        },
                        {
                            "role": "user",
                            "content": user_content
                        }
                    ],
                    temperature=0.0
                )

                content = completion.choices[0].message.content
                print(f"[DEBUG] Output for '{channel_name}':\n{content}\n---")

                try:
                    parsed = json.loads(content)
                    description = parsed.get("Description", "").strip()
                    category = parsed.get("Category", "").strip()
                except json.JSONDecodeError:
                    print(f"[ERROR] Invalid JSON for '{channel_name}': {content!r}")
                    description = "Error retrieving description"
                    category = "Uncategorized"

                if category not in CATEGORIES:
                    category = "Uncategorized"

            except Exception as e:
                print(f"OpenAI error for channel '{channel_name}': {e}")
                description = "Error retrieving description"
                category = "Uncategorized"

            results.append({
                "Channel": channel_name,
                "Description": description,
                "Category": category
            })
        time.sleep(1)
    return results

def load_channel_cache(sheets_service, spreadsheet_id):
    check_rate_limit()
    ss_meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    increment_read_count()

    sheet_names = [s['properties']['title'] for s in ss_meta['sheets']]
    if "Channel_Cache" not in sheet_names:
        print("No 'Channel_Cache' sheet found. Will treat as empty.")
        return {}

    check_rate_limit()
    resp = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range="Channel_Cache!A2:D"
    ).execute()
    increment_read_count()

    rows = resp.get("values", [])

    cache = {}
    for row in rows:
        while len(row) < 4:
            row.append("")
        ch, desc, cat, url = row
        if ch:
            cache[ch] = {
                "Description": desc,
                "Category": cat,
                "YouTubeURL": url
            }
    print(f"Loaded {len(cache)} channels from Channel_Cache.")
    return cache

def append_many_channels_to_cache(sheets_service, spreadsheet_id, rows_to_append):
    check_rate_limit()
    ss_meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    increment_read_count()

    sheet_names = [s['properties']['title'] for s in ss_meta['sheets']]
    if "Channel_Cache" not in sheet_names:
        print("Creating 'Channel_Cache' sheet with headers.")
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={
                "requests": [
                    {
                        "addSheet": {
                            "properties": {"title": "Channel_Cache"}
                        }
                    }
                ]
            }
        ).execute()

        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range="Channel_Cache!A1:D1",
            valueInputOption="RAW",
            body={"values": [["Channel", "Description", "Category", "YouTubeURL"]]}
        ).execute()

    # single append
    body = {"values": rows_to_append}
    sheets_service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range="Channel_Cache!A:D",
        valueInputOption="RAW",
        insertDataOption="INSERT_ROWS",
        body=body
    ).execute()

def save_progress(progress_dict):
    with open(PROGRESS_JSON, "w") as f:
        json.dump(progress_dict, f, indent=2)


if __name__ == "__main__":
    if SERP_API_KEY and SERP_API_KEY != "YOUR_SERP_API_KEY":
        print("SERP_API_KEY found.")
    else:
        print("WARNING: SERP_API_KEY not set or is placeholder!")

    if OPENAI_API_KEY and OPENAI_API_KEY != "YOUR_OPENAI_API_KEY":
        openai.api_key = OPENAI_API_KEY
        print("OPENAI_API_KEY loaded.")
    else:
        print("WARNING: OPENAI_API_KEY not set or is placeholder!")

    main()
