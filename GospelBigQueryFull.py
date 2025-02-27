import os
import re
import time
import math
import json
from dotenv import load_dotenv
from openai import OpenAI
import pandas as pd
import gspread
from serpapi import GoogleSearch
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from googleapiclient.discovery import build

# -------------------------------------------------------------------
# GOOGLE / OPENAI CONFIG
# -------------------------------------------------------------------

SERVICE_ACCOUNT_FILE = '/Users/keithjohnson/Desktop/youtube_processor/rich-stratum-367618-d50ca77c37c2.json'

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/bigquery",
    "https://www.googleapis.com/auth/cloud-platform"
]

credentials = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

gc = gspread.authorize(credentials)
drive_service = build("drive", "v3", credentials=credentials)

# 1) Load .env
load_dotenv()  # this will parse the .env file and load environment variables

# 2) Read the API keys from the environment
api_key = os.getenv("OPENAI_API_KEY", "YOUR_OPENAI_API_KEY")
SERP_API_KEY = os.getenv("SERP_API_KEY", "YOUR_SERP_API_KEY")

# 3) Instantiate the client
client = OpenAI(api_key=api_key)

# -------------------------------------------------------------------
# BIGQUERY CONFIG
# -------------------------------------------------------------------

PROJECT_ID = "rich-stratum-367618"
DATASET_ID = "GospelBrandReport"

MASTER_TABLE = "MasterTable"
CHANNEL_REF_TABLE = "ChannelDescriptionReference"
SHEETS_PROCESSED_TABLE = "SheetsProcessed"

bq_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

# -------------------------------------------------------------------
# OTHER CONSTANTS
# -------------------------------------------------------------------

FOLDER_ID = "1JTDdz7v3ohW1CC8AzUBd5xADI9dOAycu"    # Google Drive folder ID
MODIFIED_AFTER = "2024-06-21T00:00:00Z"          # For filtering by modified time

CATEGORIES = [
    "Video Games", "Entertainment", "Education", "Sports", "Technology",
    "Music", "Podcast", "Politics", "Travel", "Automotive",
    "Movies", "Humor", "Food", "Science", "Fashion",
    "Real Estate", "Photography", "Home Improvement", "Finance"
]

MASTER_TABLE_SCHEMA = [
    bigquery.SchemaField("Channel title", "STRING"),
    bigquery.SchemaField("Brands", "STRING"),
    bigquery.SchemaField("Views", "INT64"),
    bigquery.SchemaField("Video title", "STRING"),
    bigquery.SchemaField("Video URL", "STRING"),
    bigquery.SchemaField("Duration in seconds", "INT64"),
    bigquery.SchemaField("Country", "STRING"),
    bigquery.SchemaField("Language", "STRING"),
    bigquery.SchemaField("Date published", "DATE"),
    bigquery.SchemaField("Category", "STRING"),
    bigquery.SchemaField("YouTube URL", "STRING"),
    bigquery.SchemaField("All time views", "INT64"),
    bigquery.SchemaField("All time subs", "INT64"),
    bigquery.SchemaField("30 day views", "INT64"),
    bigquery.SchemaField("30 Day Subs", "INT64"),
    bigquery.SchemaField("Made_for_kids", "STRING"),
    bigquery.SchemaField("Description", "STRING"),
    bigquery.SchemaField("date", "DATE"),
]

# -------------------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------------------

def create_dataset_if_not_exists(client, dataset_ref):
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)

def table_exists(client, dataset_id, table_name):
    try:
        client.get_table(f"{dataset_id}.{table_name}")
        return True
    except:
        return False

def list_sheets_in_folder_after_date(drive_service, folder_id, modified_after):
    """
    Return a list of (sheet_name, sheet_id, modifiedTime) for Google Sheets
    in the folder, filtering by 'modifiedTime > modified_after'.
    """
    query = (
        f"'{folder_id}' in parents "
        f"and mimeType='application/vnd.google-apps.spreadsheet' "
        f"and trashed=false "
        f"and modifiedTime > '{modified_after}'"
    )
    results = drive_service.files().list(q=query, fields="files(id, name, modifiedTime)").execute()
    files = results.get("files", [])
    return [(f["name"], f["id"], f["modifiedTime"]) for f in files]

def get_processed_sheets():
    main_table_id = f"{DATASET_ID}.{SHEETS_PROCESSED_TABLE}"
    if not table_exists(bq_client, DATASET_ID, SHEETS_PROCESSED_TABLE):
        return set()
    query = f"SELECT sheet_id FROM `{main_table_id}`"
    rows = bq_client.query(query).result()
    return {r.sheet_id for r in rows}

def upsert_sheets_processed(sheet_id, sheet_name):
    create_dataset_if_not_exists(bq_client, f"{PROJECT_ID}.{DATASET_ID}")
    if not table_exists(bq_client, DATASET_ID, SHEETS_PROCESSED_TABLE):
        schema = [
            bigquery.SchemaField("sheet_id", "STRING"),
            bigquery.SchemaField("sheet_name", "STRING"),
            bigquery.SchemaField("last_loaded_timestamp", "TIMESTAMP"),
        ]
        table_ref = bigquery.Table(
            f"{PROJECT_ID}.{DATASET_ID}.{SHEETS_PROCESSED_TABLE}",
            schema=schema
        )
        bq_client.create_table(table_ref)

    temp_table_id = f"{DATASET_ID}._staging_sheets_processed_{int(time.time())}"
    df = pd.DataFrame([{
        "sheet_id": sheet_id,
        "sheet_name": sheet_name,
        "last_loaded_timestamp": pd.Timestamp.utcnow(),
    }])
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()

    main_table_id = f"{DATASET_ID}.{SHEETS_PROCESSED_TABLE}"
    merge_query = f"""
    MERGE `{main_table_id}` T
    USING `{temp_table_id}` S
    ON T.sheet_id = S.sheet_id
    WHEN MATCHED THEN
      UPDATE SET
        T.sheet_name = S.sheet_name,
        T.last_loaded_timestamp = S.last_loaded_timestamp
    WHEN NOT MATCHED THEN
      INSERT (sheet_id, sheet_name, last_loaded_timestamp)
      VALUES (S.sheet_id, S.sheet_name, S.last_loaded_timestamp)
    """
    bq_client.query(merge_query).result()
    bq_client.delete_table(temp_table_id, not_found_ok=True)

def read_sheet_to_dataframe(sheet_id, tab_name="Videos - Raw Data"):
    sh = gc.open_by_key(sheet_id)
    worksheet = sh.worksheet(tab_name)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    return df

def clean_and_prepare_dataframe(df, ingestion_date=None):
    df.columns = [c.strip() for c in df.columns]
    rename_map = {
        'Made for kids?': 'Made_for_kids'
    }
    df.rename(columns=rename_map, inplace=True)

    numeric_cols = [
        "Views", 
        "Duration in seconds", 
        "All time views",
        "All time subs", 
        "30 day views", 
        "30 Day Subs"
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            df[col] = df[col].apply(lambda x: int(round(x)) if pd.notnull(x) else None)
            df[col] = df[col].astype("Int64")

    if "Date published" in df.columns:
        df["Date published"] = pd.to_datetime(df["Date published"], errors="coerce").dt.date

    for col in df.columns:
        if col not in numeric_cols and col != "Date published":
            df[col] = df[col].astype(str)

    if "date" not in df.columns:
        df["date"] = ingestion_date if ingestion_date else pd.Timestamp.utcnow().date()

    master_col_names = {field.name for field in MASTER_TABLE_SCHEMA}
    df = df[[c for c in df.columns if c in master_col_names]]

    return df

def upsert_to_master_table(df, project_id, dataset_id, table_name, unique_key_cols=["Channel title", "Video URL"]):
    df = df.drop_duplicates(subset=unique_key_cols, keep="last")
    create_dataset_if_not_exists(bq_client, f"{project_id}.{dataset_id}")

    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    if not table_exists(bq_client, dataset_id, table_name):
        table_ref = bigquery.Table(full_table_id, schema=MASTER_TABLE_SCHEMA)
        bq_client.create_table(table_ref)
        print(f"Created {full_table_id} with provided schema.")

    temp_table_id = f"{dataset_id}._staging_{table_name}_{int(time.time())}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()

    on_clause = " AND ".join([f"T.`{col}` = S.`{col}`" for col in unique_key_cols if col in df.columns])
    update_cols = [col for col in df.columns if col not in unique_key_cols]
    update_set = ", ".join([f"T.`{col}` = S.`{col}`" for col in update_cols])
    insert_cols = ", ".join([f"`{col}`" for col in df.columns])
    insert_vals = ", ".join([f"S.`{col}`" for col in df.columns])

    merge_query = f"""
    MERGE `{full_table_id}` T
    USING `{project_id}.{temp_table_id}` S
    ON {on_clause}
    WHEN MATCHED THEN
      UPDATE SET {update_set}
    WHEN NOT MATCHED THEN
      INSERT ({insert_cols})
      VALUES ({insert_vals})
    """
    bq_client.query(merge_query).result()
    bq_client.delete_table(f"{project_id}.{temp_table_id}", not_found_ok=True)

    print(f"Upserted {len(df)} rows into {full_table_id}.")

def identify_uncategorized_channels():
    query = f"""
    SELECT DISTINCT m.`Channel title`
    FROM `{PROJECT_ID}.{DATASET_ID}.{MASTER_TABLE}` m
    LEFT JOIN `{PROJECT_ID}.{DATASET_ID}.{CHANNEL_REF_TABLE}` c
      ON m.`Channel title` = c.Channel
    WHERE c.Channel IS NULL
       OR c.Category IS NULL
       OR c.Category = ''
       OR c.Category = 'Uncategorized'
    """
    results = bq_client.query(query).result()
    return [row[0] for row in results if row[0]]

def fetch_channel_info(channel_name):
    """
    Use SerpAPI to find info about the given YouTube channel.
    """
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
        for res in results["organic_results"]:
            title = res.get("title", "")
            snippet_text = res.get("snippet", "")
            link = res.get("link", "")
            if "youtube.com" in link.lower() or "channel" in title.lower():
                snippet += f"* {title}: {snippet_text}\n"

    return snippet.strip()

def classify_channels_with_openai(channel_list, batch_size=10):
    """
    For each channel in the list, gather info from SerpAPI + model classification.
    """
    results = []
    for i in range(0, len(channel_list), batch_size):
        batch = channel_list[i : i + batch_size]
        for channel_name in batch:
            serp_snippet = fetch_channel_info(channel_name)
            # If you want to debug the SerpAPI snippet:
            # print(f"[DEBUG] Serp snippet for {channel_name}:\n{serp_snippet}\n")

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
                    model="gpt-4o",  # or "gpt-3.5-turbo", "gpt-4", etc.
                    messages=[
                        {
                            "role": "developer",
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

                # 1) Debug print to see EXACTLY what the model returned:
                print(f"[DEBUG] Model output for '{channel_name}':\n{content!r}\n---")

                # 2) Parse JSON
                try:
                    parsed = json.loads(content)
                    description = parsed.get("Description", "").strip()
                    category = parsed.get("Category", "").strip()
                except json.JSONDecodeError:
                    # The model didn't return valid JSON
                    print(f"[ERROR] Invalid JSON from model for '{channel_name}': {content!r}")
                    description = "Error retrieving description"
                    category = "Uncategorized"

                # 3) Validate category
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

def upsert_channel_descriptions(channel_data):
    if not channel_data:
        print("No channel data to upsert.")
        return

    create_dataset_if_not_exists(bq_client, f"{PROJECT_ID}.{DATASET_ID}")

    full_ref_id = f"{PROJECT_ID}.{DATASET_ID}.{CHANNEL_REF_TABLE}"
    if not table_exists(bq_client, DATASET_ID, CHANNEL_REF_TABLE):
        schema = [
            bigquery.SchemaField("Channel", "STRING"),
            bigquery.SchemaField("Description", "STRING"),
            bigquery.SchemaField("Category", "STRING"),
        ]
        table_ref = bigquery.Table(full_ref_id, schema=schema)
        bq_client.create_table(table_ref)
        print(f"Created table {CHANNEL_REF_TABLE}.")

    df = pd.DataFrame(channel_data)

    temp_table_id = f"{DATASET_ID}._staging_{CHANNEL_REF_TABLE}_{int(time.time())}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()

    merge_query = f"""
    MERGE `{full_ref_id}` T
    USING `{PROJECT_ID}.{temp_table_id}` S
    ON T.Channel = S.Channel
    WHEN MATCHED THEN
      UPDATE SET
        T.Description = S.Description,
        T.Category = S.Category
    WHEN NOT MATCHED THEN
      INSERT (Channel, Description, Category)
      VALUES (S.Channel, S.Description, S.Category)
    """
    bq_client.query(merge_query).result()
    bq_client.delete_table(f"{PROJECT_ID}.{temp_table_id}", not_found_ok=True)

    print(f"Upserted {len(df)} channels into {CHANNEL_REF_TABLE}.")

def update_master_with_channel_info():
    merge_query = f"""
    MERGE `{PROJECT_ID}.{DATASET_ID}.{MASTER_TABLE}` M
    USING `{PROJECT_ID}.{DATASET_ID}.{CHANNEL_REF_TABLE}` C
    ON M.`Channel title` = C.Channel
    WHEN MATCHED THEN
      UPDATE SET
        M.Category = C.Category,
        M.Description = C.Description
    """
    bq_client.query(merge_query).result()
    print("MasterTable updated with channel categories and descriptions.")

def main():
    create_dataset_if_not_exists(bq_client, f"{PROJECT_ID}.{DATASET_ID}")

    sheets = list_sheets_in_folder_after_date(drive_service, FOLDER_ID, MODIFIED_AFTER)
    processed = get_processed_sheets()

    for sheet_name, sheet_id, modified_time in sheets:
        if sheet_id in processed:
            print(f"Sheet '{sheet_name}' ({sheet_id}) already processed. Skipping.")
            continue

        print(f"\nProcessing sheet: {sheet_name} ({sheet_id}) ...")
        print(f"Last modified time: {modified_time}")
        try:
            df = read_sheet_to_dataframe(sheet_id, tab_name="Videos - Raw Data")
        except Exception as ex:
            print(f"Failed to read 'Videos - Raw Data' from '{sheet_name}': {ex}")
            continue

        ingestion_date = pd.Timestamp.utcnow().date()
        df = clean_and_prepare_dataframe(df, ingestion_date=ingestion_date)

        if df.empty:
            print(f"No valid data in sheet '{sheet_name}'. Skipping upload.")
            continue

        upsert_to_master_table(df, PROJECT_ID, DATASET_ID, MASTER_TABLE)
        upsert_sheets_processed(sheet_id, sheet_name)

    channels_to_classify = identify_uncategorized_channels()
    if channels_to_classify:
        print(f"Found {len(channels_to_classify)} channel(s) needing category assignment.")
        channel_data = classify_channels_with_openai(channels_to_classify)
        upsert_channel_descriptions(channel_data)
        update_master_with_channel_info()
    else:
        print("No new channels need classification.")

    print("\nAll done!")

if __name__ == "__main__":
    main()
