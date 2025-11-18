import boto3
import csv
import io
import urllib.parse
from datetime import datetime, timedelta

s3 = boto3.client("s3")

# Try several common date formats
DATE_FORMATS = ["%Y-%m-%d", "%m/%d/%Y", "%Y/%m/%d", "%d-%m-%Y"]

def parse_date(s):
    for fmt in DATE_FORMATS:
        try:
            return datetime.strptime(s.strip(), fmt)
        except Exception:
            pass
    raise ValueError(f"Unrecognized date format: {s!r}")

def lambda_handler(event, context):
    print("Lambda triggered by S3 event.")

    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key_encoded = event["Records"][0]["s3"]["object"]["key"]
    raw_key = urllib.parse.unquote_plus(key_encoded, encoding="utf-8")
    print(f"Incoming object: s3://{bucket}/{raw_key}")

    # Read the CSV from S3
    try:
        obj = s3.get_object(Bucket=bucket, Key=raw_key)
        lines = obj["Body"].read().decode("utf-8").splitlines()
    except Exception as e:
        print(f"Error reading S3 object: {e}")
        raise

    if not lines:
        print("Empty file; nothing to process.")
        return {"statusCode": 200, "body": "Empty file"}

    reader = csv.DictReader(lines)
    # Normalize headers -> lowercase without spaces/underscores
    header_map = { (h or "").strip().lower().replace(" ", "").replace("_", ""): h for h in (reader.fieldnames or []) }

    # Find likely columns for status and order date
    status_key = None
    for cand in ("status", "orderstatus"):
        if cand in header_map:
            status_key = header_map[cand]
            break

    date_key = None
    for cand in ("orderdate", "date", "order_date"):
        cand_norm = cand.replace("_", "")
        if cand_norm in header_map:
            date_key = header_map[cand_norm]
            break

    if not status_key or not date_key:
        print(f"Could not find required columns. Headers={reader.fieldnames}")
        raise KeyError("Missing 'Status'/'OrderDate' (case-insensitive variants accepted)")

    kept_rows = []
    total = 0
    filtered_out = 0
    cutoff = datetime.now() - timedelta(days=30)

    print(f"Using columns: status='{status_key}', date='{date_key}', cutoff={cutoff.date()}")

    for row in reader:
        total += 1
        status_raw = (row.get(status_key) or "").strip().lower()
        date_raw = (row.get(date_key) or "").strip()
        try:
            order_dt = parse_date(date_raw)
        except Exception as e:
            print(f"Row {total}: bad date {date_raw!r} -> skipping. Error: {e}")
            filtered_out += 1
            continue

        # Keep logic (your original rule):
        # Keep if NOT (pending/cancelled and older than 30 days)
        if status_raw not in {"pending", "cancelled"} or order_dt > cutoff:
            kept_rows.append(row)
        else:
            filtered_out += 1

    print(f"Processed rows: {total}, kept: {len(kept_rows)}, filtered_out: {filtered_out}")

    # Write output CSV
    out_buf = io.StringIO()
    writer = csv.DictWriter(out_buf, fieldnames=reader.fieldnames)
    writer.writeheader()
    if kept_rows:
        writer.writerows(kept_rows)

    # Save under processed/ mirroring the incoming name
    file_name = raw_key.split("/")[-1]
    processed_key = raw_key.replace("raw/", "processed/", 1)
    # Or: processed_key = f"processed/filtered_{file_name}"

    try:
        s3.put_object(
            Bucket=bucket,
            Key=processed_key,
            Body=out_buf.getvalue().encode("utf-8"),
            ContentType="text/csv; charset=utf-8",
        )
        print(f"Wrote {len(kept_rows)} rows to s3://{bucket}/{processed_key}")
    except Exception as e:
        print(f"Error writing processed CSV: {e}")
        raise

    return {
        "statusCode": 200,
        "body": f"Kept {len(kept_rows)} of {total} rows -> {processed_key}"
    }
