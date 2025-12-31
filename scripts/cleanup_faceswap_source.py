import os
import boto3

# Load env variables
DO_SPACES_KEY = os.getenv("DO_SPACES_KEY")
DO_SPACES_SECRET = os.getenv("DO_SPACES_SECRET")
DO_SPACES_REGION = os.getenv("DO_SPACES_REGION")
DO_SPACES_ENDPOINT = os.getenv("DO_SPACES_ENDPOINT")
DO_SPACES_BUCKET = os.getenv("DO_SPACES_BUCKET")

PREFIX = "faceswap/source/"

def main():
    session = boto3.session.Session()

    client = session.client(
        "s3",
        region_name=DO_SPACES_REGION,
        endpoint_url=DO_SPACES_ENDPOINT,
        aws_access_key_id=DO_SPACES_KEY,
        aws_secret_access_key=DO_SPACES_SECRET,
    )

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=DO_SPACES_BUCKET, Prefix=PREFIX)

    objects_to_delete = []

    for page in pages:
        for obj in page.get("Contents", []):
            key = obj["Key"]

            # Safety check — delete only files inside source/
            if key.startswith(PREFIX) and key != PREFIX:
                objects_to_delete.append({"Key": key})

    if not objects_to_delete:
        print("✅ No files to delete.")
        return

    # Batch delete (max 1000 per request)
    for i in range(0, len(objects_to_delete), 1000):
        batch = objects_to_delete[i:i + 1000]
        client.delete_objects(
            Bucket=DO_SPACES_BUCKET,
            Delete={"Objects": batch}
        )

    print(f"🗑️ Deleted {len(objects_to_delete)} files from {PREFIX}")

if __name__ == "__main__":
    main()
