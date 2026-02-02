# infomed-html-parser

Parser for ANSM medication HTML documents (Notices and RCPs).

## Installation

```bash
poetry install
```

## Usage

The CLI supports two modes: **local** (for development) and **s3** (for production).

### Local Mode

Process HTML files from a local directory:

```bash
poetry run infomed-html-parser local <html_folder> [options]
```

Arguments:
- `html_folder`: Directory containing HTML files (N*.htm for Notices, R*.htm for RCPs)

Options:
- `--cis-file`: Text file with allowed CIS codes (default: uses database)
- `--output, -o`: Output JSONL file (default: output.jsonl)
- `--limite`: Limit number of files to process (for testing)
- `--processes`: Number of parallel processes (default: CPU count)
- `--pattern`: File pattern - N=Notice, R=RCP, NR=both (default: N)

Example:
```bash
# Uses database for CIS list (Specialite.isBdm)
poetry run infomed-html-parser local ./html_files -o output.jsonl --pattern NR

# With CIS file override
poetry run infomed-html-parser local ./html_files --cis-file cis_list.txt -o output.jsonl
```

### S3 Mode

Process HTML files from S3 (Clever Cloud Cellar) and write results back to S3:

```bash
poetry run infomed-html-parser s3 [options]
```

Options:
- `--cis-file`: Text file with allowed CIS codes (default: uses database)
- `--limite`: Limit number of files to process (for testing)
- `--pattern`: File pattern - N=Notice, R=RCP, NR=both (default: N)

Example:
```bash
poetry run infomed-html-parser s3 --pattern R --limite 100
```

### Global Options

- `--verbose, -v`: Enable debug logging

## Environment Variables

### S3/Cellar Configuration

- `S3_HOST`: S3 endpoint URL (default: https://cellar-c2.services.clever-cloud.com)
- `S3_KEY_ID`: S3 access key (required for S3 mode)
- `S3_KEY_SECRET`: S3 secret key (required for S3 mode)
- `S3_BUCKET_NAME`: Bucket name (default: info-medicaments)
- `S3_HTML_PREFIX`: Prefix for HTML files (default: exports/html/)
- `S3_OUTPUT_PREFIX`: Prefix for output files (default: exports/parsed/)

### Database Configuration

The database is used for two purposes:
1. **CIS list**: By default, authorized CIS codes are loaded from `SELECT SpecId FROM Specialite WHERE isBdm`
2. **Filename mapping**: Maps HTML filenames to CIS codes via the `Spec_Doc` and `Document` tables

Environment variables:
- `MYSQL_HOST` (default: localhost)
- `MYSQL_USER` (default: root)
- `MYSQL_PASSWORD` (default: mysql)
- `MYSQL_DATABASE` (default: pdbm_bdd)
- `MYSQL_PORT` (default: 3306)

### Application Configuration

- `LOG_LEVEL`: Logging level (default: INFO)
- `CDN_BASE_URL`: Base URL for image CDN (default: https://cellar-c2.services.clever-cloud.com/info-medicaments/exports/images)

## Scalingo Deployment

This project is designed to run as scheduled tasks on Scalingo.

### Procfile

The `Procfile` defines three task types:

```
parse-notices: python -m infomed_html_parser.cli s3 --pattern N
parse-rcp: python -m infomed_html_parser.cli s3 --pattern R
parse-all: python -m infomed_html_parser.cli s3 --pattern NR
```

### Running Tasks

Use Scalingo Scheduler or run as one-off containers:

```bash
# Run via Scalingo CLI
scalingo --app your-app run parse-notices
scalingo --app your-app run parse-rcp
```

### Required Environment Variables on Scalingo

Set these in your Scalingo app settings:
- `S3_KEY_ID` and `S3_KEY_SECRET` (from Clever Cloud Cellar addon)
- `MYSQL_HOST`, `MYSQL_USER`, `MYSQL_PASSWORD`, `MYSQL_DATABASE` (database connection)

## Development

```bash
# Install with dev dependencies
poetry install --with dev

# Run tests
poetry run pytest

# Run tests with coverage
poetry run pytest --cov=infomed_html_parser

# Lint and format
poetry run ruff check .
poetry run ruff format .

# Auto-fix linting issues
poetry run ruff check . --fix
```
