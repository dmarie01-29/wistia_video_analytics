import sys
import json
import boto3
import requests
from datetime import datetime
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions

# 1. Load dynamic infrastructure arguments from Glue runtime profile
args = getResolvedOptions(sys.argv, ['BUCKET_NAME', 'SECRET_NAME'])
BUCKET_NAME = args['BUCKET_NAME']
SECRET_NAME = args['SECRET_NAME']

# Target assignment project Media IDs
MEDIA_IDS = ["8hunphufxp", "9k4tbcdfg0"]

s3_client = boto3.client('s3')
secrets_client = boto3.client('secretsmanager')


def get_wistia_token(secret_id):
    """Safely extracts token strings from AWS Secrets Manager vaults."""
    try:
        response = secrets_client.get_secret_value(SecretId=secret_id)
        return json.loads(response['SecretString'])['wistia_api_token']
    except ClientError as e:
        print(f"Secret Fetch Error: {e}")
        raise e


def get_pipeline_state(bucket, key="state/checkpoint.json"):
    """Tracks incremental time boundaries to optimize pipeline delta parsing."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return json.loads(response['Body'].read().decode('utf-8'))
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            return {"last_successful_ingestion": "1970-01-01T00:00:00Z"}
        raise e


def update_pipeline_state(bucket, new_timestamp, key="state/checkpoint.json"):
    """Commits fresh processing time markers back to S3 tracking files."""
    s3_client.put_object(
        Bucket=bucket, Key=key,
        Body=json.dumps({"last_successful_ingestion": new_timestamp}, indent=4).encode('utf-8')
    )


def get_media_title(media_id, token):
    """Looks up the display name for a media item once, so it can be stamped onto every event."""
    url = f"https://api.wistia.com/v1/medias/{media_id}.json"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json().get("name", "Unknown Title")
    print(f"Could not fetch title for {media_id}: {response.status_code} - {response.text[:200]}")
    return "Unknown Title"


def fetch_paginated_visitor_events(media_id, media_title, token, bucket, since_timestamp):
    """FR5 & FR6: Fetches paginated visitor logs from the Wistia Stats Events endpoint."""
    # Wistia's current Events endpoint — media_id is a query param here, not a path segment
    base_url = "https://api.wistia.com/modern/stats/events"
    headers = {
        "Authorization": f"Bearer {token}",
        "X-Wistia-API-Version": "2026-07",  # required header; defaults server-side if omitted, but pin it explicitly
    }

    page = 1
    per_page = 100  # Wistia caps this endpoint at 100
    has_more_data = True
    current_date = datetime.utcnow().strftime("%Y-%m-%d")

    while has_more_data:
        params = {
            "media_id": media_id,
            "page": page,
            "per_page": per_page
        }

        print(f"Polling Page {page} for Video {media_id} from Wistia Events API...")
        response = requests.get(base_url, headers=headers, params=params)

        # Guard against non-JSON responses (HTML error pages, redirects, etc.)
        # before ever calling .json() — this is what turned into a JSONDecodeError last time.
        if response.status_code == 200:
            if "application/json" not in response.headers.get("Content-Type", ""):
                print(
                    f"Unexpected content type from {media_id} page {page}: "
                    f"{response.headers.get('Content-Type')}. Body preview: {response.text[:200]}"
                )
                has_more_data = False
                continue

            events_list = response.json()

            # If the response list is empty, we reached the end of Wistia's pagination loop
            if not events_list or len(events_list) == 0:
                print(f"No further records discovered for {media_id} on Page {page}.")
                break

            # Filter and store events incrementally matching our state file boundary
            filtered_events = []
            for event in events_list:
                event_time = event.get("received_at", event.get("created_at", "1970-01-01T00:00:00Z"))
                if event_time > since_timestamp:
                    event['target_media_id'] = media_id
                    event['media_name'] = media_title
                    event['extracted_at'] = datetime.utcnow().isoformat() + "Z"
                    filtered_events.append(event)

            # Save the batched data page to S3 if it contains fresh delta tracking events
            if len(filtered_events) > 0:
                s3_key = f"landing/media_stats/load_date={current_date}/{media_id}_events_page_{page}.json"
                s3_client.put_object(
                    Bucket=bucket, Key=s3_key,
                    Body=json.dumps(filtered_events, indent=4).encode('utf-8')
                )
                print(f"Committed {len(filtered_events)} tracking rows to S3 path: {s3_key}")

            # If the API returned fewer records than per_page, we are on the final page
            if len(events_list) < per_page:
                has_more_data = False
            else:
                page += 1
        else:
            print(f"API Error on {media_id} (Page {page}): {response.status_code} - {response.text[:200]}")
            has_more_data = False


def main():
    print("Initiating Enhanced Granular Visitor-Level Ingestion Pipeline Core...")
    api_token = get_wistia_token(SECRET_NAME)

    current_state = get_pipeline_state(BUCKET_NAME)
    since_timestamp = current_state["last_successful_ingestion"]
    print(f"Incremental Ingestion Threshold: Extracting rows created after {since_timestamp}")

    for media_id in MEDIA_IDS:
        media_title = get_media_title(media_id, api_token)
        fetch_paginated_visitor_events(media_id, media_title, api_token, BUCKET_NAME, since_timestamp)

    execution_window_marker = datetime.utcnow().isoformat() + "Z"
    update_pipeline_state(BUCKET_NAME, execution_window_marker)
    print("Phase 1 Ingestion Completed Successfully.")


if __name__ == "__main__":
    main()