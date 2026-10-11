#!/usr/bin/env python3
"""Usage examples:
  # List everything in a run
  python3 restore.py --bucket soabcr --prefix archive --run-id run-20261011T... --list

  # Restore one file
  python3 restore.py --bucket soabcr --prefix archive --run-id run-... --file "reports/file.csv" --dest ./restored

  # Restore everything
  python3 restore.py --bucket soabcr --prefix archive --run-id run-... --all --dest ./restored
"""
from __future__ import annotations
import argparse
import os
import sqlite3
import sys
import tempfile
import zipfile
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError
except ImportError:
    print("boto3 is required: python3 -m pip install boto3", file=sys.stderr)
    sys.exit(1)


def get_s3(profile: str | None, region: str | None):
    session = boto3.Session(profile_name=profile, region_name=region)
    return session.client("s3")


def download_manifest(s3, bucket: str, prefix: str, run_id: str, dest: Path) -> Path:
    key = f"{prefix.strip('/')}/{run_id}/manifest/files.sqlite"
    dest.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading manifest s3://{bucket}/{key}")
    s3.download_file(bucket, key, str(dest))
    return dest


def list_files(db_path: Path) -> None:
    con = sqlite3.connect(db_path)
    rows = con.execute(
        "SELECT path, size_bytes, storage_tier, object_key, zip_member FROM files ORDER BY path"
    ).fetchall()
    print(f"{'PATH':<60} {'SIZE':>12} {'TIER':<8} {'IN ZIP?'}")
    print("-" * 100)
    for path, size, tier, key, member in rows:
        in_zip = "yes" if member else "no"
        print(f"{path:<60} {size:>12} {tier:<8} {in_zip}")
    print(f"\nTotal files: {len(rows)}")
    con.close()


def ensure_restored(s3, bucket: str, key: str, storage_class: str) -> bool:
    """For Deep Archive objects, initiate a restore if needed.
    Returns True if the object is immediately downloadable.
    """
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        print(f"ERROR: cannot head {key}: {e}", file=sys.stderr)
        return False

    # Glacier Instant Retrieval is always available
    if storage_class == "GLACIER_IR":
        return True

    # Deep Archive needs an explicit restore
    restore_status = head.get("Restore", "")
    if 'ongoing-request="true"' in restore_status:
        print(f"  Restore already in progress for {key}")
        return False
    if 'ongoing-request="false"' in restore_status:
        return True  # temporarily restored

    print(f"  Initiating Deep Archive restore for {key} (this can take 12-48 h)")
    s3.restore_object(
        Bucket=bucket,
        Key=key,
        RestoreRequest={
            "Days": 7,
            "GlacierJobParameters": {"Tier": "Standard"},
        },
    )
    return False


def restore_file(
    s3,
    bucket: str,
    db_path: Path,
    relative: str,
    dest_root: Path,
) -> None:
    con = sqlite3.connect(db_path)
    row = con.execute(
        "SELECT object_key, zip_member, storage_tier FROM files WHERE path = ?",
        (relative,),
    ).fetchone()
    con.close()

    if not row:
        print(f"File not found in manifest: {relative}", file=sys.stderr)
        return

    object_key, zip_member, tier = row
    storage_class = "GLACIER_IR" if tier == "instant" else "DEEP_ARCHIVE"

    if not ensure_restored(s3, bucket, object_key, storage_class):
        print(f"  Skipping {relative} – object not yet available for download")
        return

    dest = dest_root / relative
    dest.parent.mkdir(parents=True, exist_ok=True)

    if zip_member is None:
        # Individual object
        print(f"Downloading {relative}")
        s3.download_file(bucket, object_key, str(dest))
    else:
        # Inside a zip – download zip to temp, extract one member
        with tempfile.TemporaryDirectory() as tmp:
            zip_path = Path(tmp) / "chunk.zip"
            print(f"Downloading zip for {relative}")
            s3.download_file(bucket, object_key, str(zip_path))
            with zipfile.ZipFile(zip_path, "r") as zf:
                with zf.open(zip_member) as src, open(dest, "wb") as out:
                    out.write(src.read())
        print(f"Extracted {relative}")


def restore_all(s3, bucket: str, db_path: Path, dest_root: Path) -> None:
    con = sqlite3.connect(db_path)
    rows = con.execute("SELECT path FROM files ORDER BY path").fetchall()
    con.close()
    for (path,) in rows:
        restore_file(s3, bucket, db_path, path, dest_root)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--bucket", required=True)
    p.add_argument("--prefix", required=True)
    p.add_argument("--run-id", required=True)
    p.add_argument("--profile", default=None)
    p.add_argument("--region", default=None)
    p.add_argument("--manifest", type=Path, help="Local path to already-downloaded files.sqlite")
    p.add_argument("--list", action="store_true", help="Just list files in the run")
    p.add_argument("--file", help="Restore a single relative path")
    p.add_argument("--all", action="store_true", help="Restore every file")
    p.add_argument("--dest", type=Path, default=Path("./restored"))
    args = p.parse_args()

    s3 = get_s3(args.profile, args.region)

    if args.manifest:
        db_path = args.manifest
    else:
        db_path = Path(f"/tmp/{args.run_id}-files.sqlite")
        download_manifest(s3, args.bucket, args.prefix, args.run_id, db_path)

    if args.list:
        list_files(db_path)
        return 0

    if args.file:
        restore_file(s3, args.bucket, db_path, args.file, args.dest)
        return 0

    if args.all:
        restore_all(s3, args.bucket, db_path, args.dest)
        return 0

    print("Nothing to do. Use --list, --file or --all.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())