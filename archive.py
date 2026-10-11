#!/usr/bin/env python3
"""
Object layout (single bucket):
  <prefix>/<run-id>/manifest/files.sqlite          STANDARD
  <prefix>/<run-id>/instant/chunk-XXXXXX.zip       GLACIER_IR
  <prefix>/<run-id>/instant/files/...              GLACIER_IR
  <prefix>/<run-id>/deep/chunk-XXXXXX.zip          DEEP_ARCHIVE
  <prefix>/<run-id>/deep/files/...                 DEEP_ARCHIVE
"""
from __future__ import annotations
import argparse
import fnmatch
import hashlib
import json
import logging
import os
import shutil
import sqlite3
import sys
import time
import uuid
import zipfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

try:
    import boto3
    from botocore.exceptions import BotoCoreError, ClientError
except ImportError:  # Keeps --dry-run usable before boto3 is installed.
    boto3 = None
    BotoCoreError = ClientError = Exception

CHUNK_BYTES = 1024 * 1024

DEFAULT_CONFIG = {
    "source_dir": "~/Downloads",
    "work_dir": "~/s3-archive-work",
    "bucket": "soabcr",
    "aws_profile": None,
    "aws_region": None,
    "prefix": "archive",
    "zip_chunk_bytes": 268435456,        # 256 MiB
    "small_file_bytes": 1048576,         # 1 MiB
    "instant_extensions": [
        ".pdf", ".txt", ".md", ".csv", ".json",
        ".doc", ".docx", ".xls", ".xlsx",
    ],
    "instant_globs": [],
    "follow_symlinks": False,
    "exclude_globs": [
        ".DS_Store",
        "**/node_modules/**",
        "**/node_modules"
    ],
}

log = logging.getLogger("archive")


def iso_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def sha256_file(path: Path) -> tuple[str, int]:
    digest, total = hashlib.sha256(), 0
    with path.open("rb") as source:
        for block in iter(lambda: source.read(CHUNK_BYTES), b""):
            digest.update(block)
            total += len(block)
    return digest.hexdigest(), total


def expanded(value: str) -> Path:
    return Path(os.path.expanduser(value)).resolve()


def read_config(path: Path) -> dict:
    with path.open(encoding="utf-8") as f:
        supplied = json.load(f)
    unknown = set(supplied) - set(DEFAULT_CONFIG)
    if unknown:
        raise ValueError("Unknown configuration key(s): " + ", ".join(sorted(unknown)))
    config = {**DEFAULT_CONFIG, **supplied}
    if not isinstance(config["bucket"], str) or not config["bucket"]:
        raise ValueError("Set 'bucket' in the config file.")
    if config["zip_chunk_bytes"] < 1024 * 1024:
        raise ValueError("zip_chunk_bytes must be at least 1 MiB.")
    return config


def is_excluded(relative: str, name: str, patterns: list[str]) -> bool:
    for pat in patterns:
        if fnmatch.fnmatch(relative, pat) or fnmatch.fnmatch(name, pat):
            return True
        if any(fnmatch.fnmatch(part, pat.rstrip("/**").rstrip("/*")) for part in relative.split("/")):
            return True
    return False


def iter_files(source: Path, config: dict) -> Iterator[Path]:
    for root, dirs, names in os.walk(source, followlinks=config["follow_symlinks"]):
        root_path = Path(root)

        if not config["follow_symlinks"]:
            dirs[:] = [d for d in dirs if not (root_path / d).is_symlink()]

        kept_dirs = []
        for d in dirs:
            rel_dir = (root_path / d).relative_to(source).as_posix()
            if is_excluded(rel_dir, d, config["exclude_globs"]):
                continue
            kept_dirs.append(d)
        dirs[:] = kept_dirs

        for name in sorted(names):
            file = root_path / name
            relative = file.relative_to(source).as_posix()

            if is_excluded(relative, name, config["exclude_globs"]):
                continue

            if file.is_symlink() and not config["follow_symlinks"]:
                log.info("SKIP symlink: %s", relative)
                continue

            try:
                if not file.is_file():
                    continue
                with file.open("rb"):
                    pass
            except OSError as e:
                log.warning("SKIP unreadable: %s (%s)", relative, e)
                continue

            yield file


def storage_class(file: Path, source: Path, config: dict) -> str:
    relative = file.relative_to(source).as_posix()
    if (file.stat().st_size <= config["small_file_bytes"]
            or file.suffix.lower() in {x.lower() for x in config["instant_extensions"]}
            or any(fnmatch.fnmatch(relative, pat) for pat in config["instant_globs"])):
        return "instant"
    return "deep"


def new_db(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(path)
    connection.executescript("""
        PRAGMA journal_mode=WAL;
        CREATE TABLE runs (
          run_id TEXT PRIMARY KEY,
          created_at TEXT,
          source_dir TEXT,
          config_json TEXT
        );
        CREATE TABLE files (
          path TEXT PRIMARY KEY,
          size_bytes INTEGER NOT NULL,
          sha256 TEXT NOT NULL,
          storage_tier TEXT NOT NULL,
          object_key TEXT NOT NULL,
          zip_member TEXT,
          archived_at TEXT NOT NULL
        );
        CREATE INDEX files_sha256_idx ON files(sha256);
        CREATE TABLE objects (
          object_key TEXT PRIMARY KEY,
          bucket TEXT NOT NULL,
          storage_class TEXT NOT NULL,
          size_bytes INTEGER NOT NULL,
          sha256 TEXT NOT NULL,
          verified_at TEXT NOT NULL
        );
    """)
    return connection


@dataclass
class Pending:
    path: Path
    relative: str
    tier: str


class Archiver:
    def __init__(self, config: dict, run_id: str, upload: bool):
        self.config, self.run_id, self.upload = config, run_id, upload
        self.source = expanded(config["source_dir"])
        self.work = expanded(config["work_dir"]) / run_id
        self.work.mkdir(parents=True, exist_ok=False)
        self.db_path = self.work / "files.sqlite"
        self.db = new_db(self.db_path)
        self.db.execute(
            "INSERT INTO runs VALUES (?, ?, ?, ?)",
            (run_id, iso_now(), str(self.source), json.dumps(config, sort_keys=True)),
        )
        self.db.commit()
        self.s3 = None
        if upload:
            if boto3 is None:
                raise RuntimeError(
                    "boto3 is required for --upload. Install it with: python3 -m pip install boto3"
                )
            session = boto3.Session(
                profile_name=config["aws_profile"],
                region_name=config["aws_region"],
            )
            self.s3 = session.client("s3")
        log.info("Run %s started (upload=%s)", run_id, upload)

    def object_key(self, tier: str, name: str) -> str:
        return f"{self.config['prefix'].strip('/')}/{self.run_id}/{tier}/{name}"

    def upload_and_verify(
        self, path: Path, key: str, storage_class: str, digest: str
    ) -> None:
        bucket = self.config["bucket"]
        if not self.upload:
            log.info("DRY RUN upload %s -> s3://%s/%s [%s]", path.name, bucket, key, storage_class)
            return
        try:
            log.info("Uploading %s -> s3://%s/%s [%s]", path.name, bucket, key, storage_class)
            self.s3.upload_file(
                str(path),
                bucket,
                key,
                ExtraArgs={
                    "StorageClass": storage_class,
                    "Metadata": {
                        "sha256": digest,
                        "archive-run": self.run_id,
                    },
                },
            )
            head = self.s3.head_object(Bucket=bucket, Key=key)
        except (BotoCoreError, ClientError) as error:
            raise RuntimeError(
                f"Upload/verification failed for s3://{bucket}/{key}: {error}"
            ) from error
        if (
            head["ContentLength"] != path.stat().st_size
            or head.get("Metadata", {}).get("sha256") != digest
        ):
            raise RuntimeError(
                f"Verification mismatch for s3://{bucket}/{key}; source files remain untouched."
            )
        log.info("Verified %s", key)
        self.db.execute(
            "INSERT OR REPLACE INTO objects VALUES (?, ?, ?, ?, ?, ?)",
            (key, bucket, storage_class, path.stat().st_size, digest, iso_now()),
        )
        self.db.commit()

    def record_file(
        self,
        relative: str,
        size: int,
        digest: str,
        tier: str,
        key: str,
        member: str | None,
    ) -> None:
        self.db.execute(
            "INSERT INTO files VALUES (?, ?, ?, ?, ?, ?, ?)",
            (relative, size, digest, tier, key, member, iso_now()),
        )

    def make_zip(self, tier: str, sequence: int, pending: list[Pending]) -> None:
        name = f"chunk-{sequence:06d}.zip"
        path = self.work / name
        log.info("Creating %s (%d files, tier=%s)", name, len(pending), tier)
        with zipfile.ZipFile(
            path, "w", compression=zipfile.ZIP_DEFLATED, allowZip64=True
        ) as archive:
            for item in pending:
                try:
                    digest, total = hashlib.sha256(), 0
                    info = zipfile.ZipInfo(item.relative)
                    info.date_time = time.localtime(item.path.stat().st_mtime)[:6]
                    info.compress_type = zipfile.ZIP_DEFLATED
                    with item.path.open("rb") as source, archive.open(info, "w") as member:
                        for block in iter(lambda: source.read(CHUNK_BYTES), b""):
                            digest.update(block)
                            total += len(block)
                            member.write(block)
                    self.record_file(
                        item.relative,
                        total,
                        digest.hexdigest(),
                        tier,
                        self.object_key(tier, name),
                        item.relative,
                    )
                except OSError as e:
                    log.warning("SKIP unreadable during zip: %s (%s)", item.relative, e)
                    continue
        zip_digest, _ = sha256_file(path)
        s3_class = "GLACIER_IR" if tier == "instant" else "DEEP_ARCHIVE"
        self.upload_and_verify(
            path, self.object_key(tier, name), s3_class, zip_digest
        )
        self.db.commit()

    def upload_large(self, item: Pending) -> None:
        try:
            digest, total = sha256_file(item.path)
        except OSError as e:
            log.warning("SKIP unreadable: %s (%s)", item.relative, e)
            return
        key = self.object_key(item.tier, "files/" + item.relative)
        s3_class = "GLACIER_IR" if item.tier == "instant" else "DEEP_ARCHIVE"
        self.upload_and_verify(item.path, key, s3_class, digest)
        self.record_file(item.relative, total, digest, item.tier, key, None)
        self.db.commit()

    def publish_manifest(self) -> None:
        self.db.commit()
        self.db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        digest, _ = sha256_file(self.db_path)
        key = f"{self.config['prefix'].strip('/')}/{self.run_id}/manifest/files.sqlite"
        log.info("Publishing manifest")
        self.upload_and_verify(self.db_path, key, "STANDARD", digest)

    def run(self) -> None:
        if not self.source.is_dir():
            raise ValueError(f"source_dir is not a directory: {self.source}")
        pending: dict[str, list[Pending]] = {"instant": [], "deep": []}
        pending_bytes = {"instant": 0, "deep": 0}
        sequences = {"instant": 0, "deep": 0}
        count = 0
        for path in iter_files(self.source, self.config):
            try:
                size = path.stat().st_size
            except OSError as e:
                log.warning("SKIP unreadable: %s (%s)", path.relative_to(self.source), e)
                continue

            item = Pending(
                path,
                path.relative_to(self.source).as_posix(),
                storage_class(path, self.source, self.config),
            )
            if size > self.config["small_file_bytes"]:
                self.upload_large(item)
            else:
                tier = item.tier
                if (
                    pending[tier]
                    and pending_bytes[tier] + size > self.config["zip_chunk_bytes"]
                ):
                    sequences[tier] += 1
                    self.make_zip(tier, sequences[tier], pending[tier])
                    pending[tier], pending_bytes[tier] = [], 0
                pending[tier].append(item)
                pending_bytes[tier] += size
            count += 1
        for tier in ("instant", "deep"):
            if pending[tier]:
                sequences[tier] += 1
                self.make_zip(tier, sequences[tier], pending[tier])
        self.publish_manifest()
        log.info(
            "Completed %s for %d files. Local manifest: %s",
            "upload" if self.upload else "dry run",
            count,
            self.db_path,
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path, help="JSON configuration file")
    parser.add_argument("--upload", action="store_true", help="Perform uploads; omit for a safe dry run")
    parser.add_argument("--keep-work", action="store_true", help="Keep generated ZIPs after a successful upload")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )

    try:
        config = read_config(args.config)
        run_id = f"run-{iso_now()}-{uuid.uuid4().hex[:8]}"
        job = Archiver(config, run_id, args.upload)
        job.run()
        if args.upload and not args.keep_work:
            shutil.rmtree(job.work)
            log.info("Removed generated local work files; originals were never modified.")
        return 0
    except (ValueError, RuntimeError, OSError, sqlite3.Error) as error:
        log.error("%s", error)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())