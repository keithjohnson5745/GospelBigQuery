import os
import re
import time
import math
import json
import openai
import pandas as pd
import gspread
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

# If you prefer, set OPENAI_API_KEY in your environment instead of placing it here
openai.api_key = os.getenv("OPENAI_API_KEY", "YOUR_OPENAI_API_KEY")

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

# Google Drive folder ID containing Sheets
FOLDER_ID = "1JTDdz7v3ohW1CC8AzUBd5xADI9dOAycu"

# (Optional) Only fetch sheets modified after this date/time (RFC 3339)
MODIFIED_AFTER = "2024-06-21T00:00:00Z"

# Categories for classification
CATEGORIES = [
    "Video Games", "Entertainment", "Education", "Sports", "Technology",
    "Music", "Podcast", "Politics", "Travel", "Automotive",
    "Movies", "Humor", "Food", "Science", "Fashion",
    "Real Estate", "Photography", "Home Improvement", "Finance"
]

# -------------------------------------------------------------------
# MASTER TABLE SCHEMA WITH INT64 FOR NUMERIC FIELDS
# -------------------------------------------------------------------
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
    bigquery.SchemaField("Description", "STRING"),  # if you want channel descriptions
    bigquery.SchemaField("date", "DATE"),           # if you want ingestion date
]

# -------------------------------------------------------------------
# HELPER FUNCTIONS
# -------------------------------------------------------------------

def create_dataset_if_not_exists(client, dataset_ref):
    """Create the dataset if it doesn't already exist."""
    dataset = bigquery.Dataset(dataset_ref)
    dataset.location = "US"
    client.create_dataset(dataset, exists_ok=True)

def table_exists(client, dataset_id, table_name):
    """Check if a BigQuery table exists."""
    try:
        client.get_table(f"{dataset_id}.{table_name}")
        return True
    except:
        return False

def list_sheets_in_folder_after_date(drive_service, folder_id, modified_after):
    """
    Return a list of (sheet_name, sheet_id, modifiedTime) for Google Sheets
    in the given folder, filtering by 'modifiedTime > modified_after'.
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
    """
    Returns a set of sheet_ids from the SheetsProcessed table 
    so we don't re-process the same sheet.
    """
    main_table_id = f"{DATASET_ID}.{SHEETS_PROCESSED_TABLE}"
    if not table_exists(bq_client, DATASET_ID, SHEETS_PROCESSED_TABLE):
        return set()

    query = f"SELECT sheet_id FROM `{main_table_id}`"
    rows = bq_client.query(query).result()
    return {r.sheet_id for r in rows}

def upsert_sheets_processed(sheet_id, sheet_name):
    """
    Insert or update a row in SheetsProcessed to mark it as processed.
    """
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
    """
    Read all rows from the specified sheet tab into a Pandas DataFrame.
    Requires gspread and authorized credentials.
    """
    sh = gc.open_by_key(sheet_id)
    worksheet = sh.worksheet(tab_name)
    data = worksheet.get_all_records()
    df = pd.DataFrame(data)
    return df

def clean_and_prepare_dataframe(df, ingestion_date=None):
    """
    Convert columns to int/string/date as needed for INT64 schema. 
    We do round->int for numeric columns. 
    """
    # 1) Strip whitespace from column names
    df.columns = [c.strip() for c in df.columns]

    # 2) Rename columns if needed
    rename_map = {
        'Made for kids?': 'Made_for_kids'
    }
    df.rename(columns=rename_map, inplace=True)

    # 3) Convert known numeric columns to integer
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
            # Convert to float
            df[col] = pd.to_numeric(df[col], errors="coerce")
            # Round and cast to int
            df[col] = df[col].apply(lambda x: int(round(x)) if pd.notnull(x) else None)
            # Optionally convert to a nullable Int64 dtype
            df[col] = df[col].astype("Int64")

    # 4) Convert 'Date published' if present
    if "Date published" in df.columns:
        df["Date published"] = pd.to_datetime(df["Date published"], errors="coerce").dt.date

    # 5) Convert everything else to string
    #    (except numeric/date columns)
    for col in df.columns:
        if col not in numeric_cols and col != "Date published":
            df[col] = df[col].astype(str)

    # 6) Add an ingestion date if not present
    if "date" not in df.columns:
        df["date"] = ingestion_date if ingestion_date else pd.Timestamp.utcnow().date()

    # 7) Keep only columns in MASTER_TABLE_SCHEMA
    master_col_names = {field.name for field in MASTER_TABLE_SCHEMA}
    df = df[[c for c in df.columns if c in master_col_names]]

    return df

def upsert_to_master_table(df, project_id, dataset_id, table_name, unique_key_cols=["Channel title", "Video URL"]):
    """Merge the DataFrame (with int columns) into the Master table (which has INT64 columns)."""
    # 1) Drop duplicates so each unique key only appears once
    df = df.drop_duplicates(subset=unique_key_cols, keep="last")

    create_dataset_if_not_exists(bq_client, f"{project_id}.{dataset_id}")

    # 2) Ensure Master table exists with INT64 schema
    full_table_id = f"{project_id}.{dataset_id}.{table_name}"
    if not table_exists(bq_client, dataset_id, table_name):
        table_ref = bigquery.Table(full_table_id, schema=MASTER_TABLE_SCHEMA)
        bq_client.create_table(table_ref)
        print(f"Created {full_table_id} with provided schema.")

    # 3) Create a temp staging table
    temp_table_id = f"{dataset_id}._staging_{table_name}_{int(time.time())}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    job = bq_client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
    job.result()

    # 4) Build MERGE statement
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

    # 5) Execute the MERGE
    bq_client.query(merge_query).result()
    bq_client.delete_table(f"{project_id}.{temp_table_id}", not_found_ok=True)

    print(f"Upserted {len(df)} rows into {full_table_id}.")

def identify_uncategorized_channels():
    """
    Return a list of channel titles that exist in MasterTable but are missing
    or not categorized in ChannelDescriptionReference.
    """
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

def classify_channels_with_openai(channel_list, batch_size=10):
    """
    For each channel in the list, request a short description + single
    category from the known CATEGORIES. This uses GPT-3.5-turbo.
    """
    results = []
    for i in range(0, len(channel_list), batch_size):
        batch = channel_list[i:i+batch_size]
        for channel_name in batch:
            prompt = f"""
You're given a YouTube channel title, and you must determine:
1) A brief description of that channel's typical content
2) Exactly one category from this list:
{', '.join(CATEGORIES)}

Return a JSON object with fields "Description" and "Category".

Channel title: {channel_name}
"""
            try:
                response = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a helpful assistant."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2
                )
                content = response["choices"][0]["message"]["content"]
                # Attempt to parse JSON
                parsed = json.loads(content)
                description = parsed.get("Description", "").strip()
                category = parsed.get("Category", "").strip()

                # Validate category
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

        # Simple rate limit buffer
        time.sleep(1)
    return results

def upsert_channel_descriptions(channel_data):
    """
    Upserts channel descriptions/categories into ChannelDescriptionReference.
    """
    if not channel_data:
        print("No channel data to upsert.")
        return

    # Ensure dataset exists
    create_dataset_if_not_exists(bq_client, f"{PROJECT_ID}.{DATASET_ID}")

    # Ensure ChannelDescriptionReference table exists
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

    # Convert list of dicts to DataFrame
    df = pd.DataFrame(channel_data)

    # Stage & merge
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
    """
    If your MasterTable includes fields for Description and Category,
    you can copy them from ChannelDescriptionReference into Master.
    """
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

# -------------------------------------------------------------------
# MAIN LOGIC
# -------------------------------------------------------------------

def main():
    # Make sure the dataset exists
    create_dataset_if_not_exists(bq_client, f"{PROJECT_ID}.{DATASET_ID}")

    # 1) Fetch all Sheets in the folder, optionally only after a date
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
            print(f"Failed to read 'Videos - Raw Data' from sheet '{sheet_name}': {ex}")
            continue

        # 2) Clean and prepare the DataFrame
        ingestion_date = pd.Timestamp.utcnow().date()
        df = clean_and_prepare_dataframe(df, ingestion_date=ingestion_date)

        if df.empty:
            print(f"No valid data in sheet '{sheet_name}'. Skipping upload.")
            continue

        # 3) Upsert data into Master table
        upsert_to_master_table(df, PROJECT_ID, DATASET_ID, MASTER_TABLE)

        # 4) Mark this sheet as processed
        upsert_sheets_processed(sheet_id, sheet_name)

    # 5) Identify uncategorized channels from Master
    channels_to_classify = identify_uncategorized_channels()
    if channels_to_classify:
        print(f"Found {len(channels_to_classify)} channel(s) needing category assignment.")
        # 6) Use OpenAI to classify
        channel_data = classify_channels_with_openai(channels_to_classify)
        # 7) Upsert channel descriptions/categories
        upsert_channel_descriptions(channel_data)
        # 8) (Optional) Update Master with new info
        update_master_with_channel_info()
    else:
        print("No new channels need classification.")

    print("\nAll done!")


if __name__ == "__main__":
    main()
