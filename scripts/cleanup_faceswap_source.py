import os
import boto3

# -------------------------------
# ENV VARIABLES
# -------------------------------
DO_SPACES_KEY = os.getenv("DO_SPACES_KEY")
DO_SPACES_SECRET = os.getenv("DO_SPACES_SECRET")
DO_SPACES_REGION = os.getenv("DO_SPACES_REGION")
DO_SPACES_ENDPOINT = os.getenv("DO_SPACES_ENDPOINT")
DO_SPACES_BUCKET = os.getenv("DO_SPACES_BUCKET")

# -------------------------------
# SAFE PREFIXES TO CLEAN
# -------------------------------
PREFIXES_TO_CLEAN = [
    "faceswap/source/",
    "faceswap/result/",
    "bikini-theme/source/",
    "bikini-theme/result/",
    "valentine/source/",
    "valentine/results/",
]

def main():
    session = boto3.session.Session()

    client = session.client(
        "s3",
        region_name=DO_SPACES_REGION,
        endpoint_url=DO_SPACES_ENDPOINT,
        aws_access_key_id=DO_SPACES_KEY,
        aws_secret_access_key=DO_SPACES_SECRET,
    )

    total_deleted = 0

    for prefix in PREFIXES_TO_CLEAN:
        print(f"\n🔍 Cleaning prefix → {prefix}")

        paginator = client.get_paginator("list_objects_v2")
        pages = paginator.paginate(
            Bucket=DO_SPACES_BUCKET,
            Prefix=prefix
        )

        objects_to_delete = []

        for page in pages:
            for obj in page.get("Contents", []):
                key = obj["Key"]

                # Hard safety guard
                if key != prefix and key.startswith(prefix):
                    objects_to_delete.append({"Key": key})

        if not objects_to_delete:
            print(f"✅ Nothing to delete in {prefix}")
            continue

        # Delete in batches (S3 max = 1000)
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i:i + 1000]
            client.delete_objects(
                Bucket=DO_SPACES_BUCKET,
                Delete={"Objects": batch}
            )

        deleted_count = len(objects_to_delete)
        total_deleted += deleted_count

        print(f"🗑️ Deleted {deleted_count} files from {prefix}")

    print(f"\n🎯 Cleanup finished. Total files deleted: {total_deleted}")

if __name__ == "__main__":
    main()
