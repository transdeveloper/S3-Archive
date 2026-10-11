# S3 Directory Archiver

Archive any local directory to a single S3 bucket.  
Source files are never modified. Dry-run is the default.

## Config (`config.json`)

```json
{
  "source_dir": "~/Downloads",
  "work_dir": "~/s3-archive-work",
  "bucket": "soabcr",
  "prefix": "archive",
  "aws_profile": "default",
  "aws_region": "eu-central-1"
}
```

## Archive

### Dry-run
```shell
python3 archive.py --config config.json
```

### Upload
```shell
python3 archive.py --config config.json --upload
```

## Restore

### List runs
```shell
aws s3 ls s3://soabcr/archive/
```

### List files in a run
```shell
python3 restore.py --bucket soabcr --prefix archive --run-id RUN_ID --list
```

### Restore one file
```shell
python3 restore.py --bucket soabcr --prefix archive --run-id RUN_ID \
  --file "path/to/file.csv" --dest ~/restored
```

### Restore everything
```shell
python3 restore.py --bucket soabcr --prefix archive --run-id RUN_ID \
  --all --dest ~/restored
```