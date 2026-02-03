"""Command-line interface for the HTML parser."""

import argparse
import glob
import json
import logging
import multiprocessing as mp
import os
from datetime import datetime

import chardet
from tqdm import tqdm

from .config import get_config
from .db import get_authorized_cis, get_filename_to_cis_mapping
from .io import charger_liste_cis
from .parser import html_vers_json
from .s3 import S3Client

logger = logging.getLogger(__name__)


def charger_html_bytes(content: bytes) -> str:
    """Decode HTML bytes with automatic encoding detection."""
    detected = chardet.detect(content)
    encoding = detected.get("encoding", "utf-8") or "utf-8"
    return content.decode(encoding)


def traiter_fichier_local(fichier_data: tuple) -> dict | None:
    """
    Process a local HTML file (function for multiprocessing).

    Args:
        fichier_data: Tuple containing (file_path, mapping, authorized_cis)

    Returns:
        Dict with JSON data or None if error/skipped
    """
    from .io import charger_html

    fichier, mapping, cis_autorises = fichier_data

    try:
        base = os.path.basename(fichier)
        cis = mapping.get(base)

        if not cis or cis not in cis_autorises:
            return None

        html = charger_html(fichier)
        data = html_vers_json(html)

        return {"source": {"filename": base, "cis": cis}, "content": data}

    except Exception as e:
        logger.error(f"Error processing {fichier}: {e}")
        return None


def traiter_fichier_s3(fichier_data: tuple) -> dict | None:
    """
    Process an HTML file from S3 (function for multiprocessing).

    Args:
        fichier_data: Tuple containing (s3_key, html_content_bytes, mapping, cis_autorises)

    Returns:
        Dict with JSON data or None if error/skipped
    """
    s3_key, html_bytes, mapping, cis_autorises = fichier_data

    try:
        filename = s3_key.split("/")[-1]
        cis = mapping.get(filename)

        if not cis or cis not in cis_autorises:
            return None

        html = charger_html_bytes(html_bytes)
        data = html_vers_json(html)

        return {"source": {"filename": filename, "cis": cis}, "content": data}

    except Exception as e:
        logger.error(f"Error processing {s3_key}: {e}")
        return None


def traiter_dossier_local(
    dossier_html: str,
    fichier_cis: str | None = None,
    fichier_sortie: str = "output.jsonl",
    limite: int | None = None,
    num_processes: int | None = None,
    pattern: str = "N",
) -> None:
    """
    Process a local folder of HTML files using multiprocessing.

    Args:
        dossier_html: Path to the folder containing HTML files
        fichier_cis: Text file containing authorized CIS codes (if None, uses database)
        fichier_sortie: Output JSONL file
        limite: Limit number of files to process (for testing)
        num_processes: Number of processes to use (default: CPU count)
        pattern: File pattern to process ("N" for Notices, "R" for RCP, "NR" for both)
    """
    if num_processes is None:
        num_processes = mp.cpu_count()

    logger.info(f"Local mode - {num_processes} processes")

    # Build glob pattern
    if pattern == "NR":
        fichiers = glob.glob(os.path.join(dossier_html, "[NR]*.htm"))
    else:
        fichiers = glob.glob(os.path.join(dossier_html, f"{pattern}*.htm"))

    if limite is not None:
        fichiers = fichiers[:limite]

    logger.info(f"{len(fichiers)} HTML files found")

    # Load CIS list from file or database
    if fichier_cis:
        cis_autorises = charger_liste_cis(fichier_cis)
        logger.info(f"CIS list loaded from file: {fichier_cis}")
    else:
        logger.info("Loading authorized CIS from database (Specialite.isBdm)...")
        cis_autorises = get_authorized_cis()
    if not cis_autorises:
        logger.error("No CIS codes loaded, stopping processing")
        return
    logger.info(f"{len(cis_autorises)} CIS codes loaded")

    logger.info("Loading filename -> CIS mapping...")
    mapping = get_filename_to_cis_mapping()
    logger.info(f"{len(mapping)} mappings loaded")

    fichiers_data = [(fichier, mapping, cis_autorises) for fichier in fichiers]

    with open(fichier_sortie, "w", encoding="utf-8") as f_out:
        pass

    files_processed = 0
    files_skipped = 0

    logger.info("Starting processing...")

    with mp.Pool(processes=num_processes) as pool:
        chunk_size = max(1, len(fichiers_data) // (num_processes * 4))

        with tqdm(total=len(fichiers_data), desc="Processing", unit="file") as pbar:
            for result in pool.imap(traiter_fichier_local, fichiers_data, chunksize=chunk_size):
                if result is not None:
                    with open(fichier_sortie, "a", encoding="utf-8") as f_out:
                        f_out.write(json.dumps(result, ensure_ascii=False) + "\n")
                    files_processed += 1
                else:
                    files_skipped += 1
                pbar.set_postfix(processed=files_processed, skipped=files_skipped)
                pbar.update(1)

    logger.info(f"Processing complete: {files_processed} processed, {files_skipped} skipped")
    logger.info(f"Output: {fichier_sortie}")


def traiter_depuis_s3(
    fichier_cis: str | None = None,
    limite: int | None = None,
    pattern: str = "N",
) -> None:
    """
    Process HTML files from S3 and write results back to S3.

    Args:
        fichier_cis: Local file containing authorized CIS codes (if None, uses database)
        limite: Limit number of files to process (for testing)
        pattern: File pattern to process ("N" for Notices, "R" for RCP)
    """
    config = get_config()

    if not config.s3.is_configured():
        raise RuntimeError("S3 credentials not configured. Set S3_KEY_ID and S3_KEY_SECRET.")

    s3_client = S3Client(config.s3)

    logger.info("S3 mode - Clever Cloud Cellar")
    logger.info(f"Bucket: {config.s3.bucket_name}")
    logger.info(f"HTML prefix: {config.s3.html_prefix}")

    # Load CIS list from file or database
    if fichier_cis:
        cis_autorises = charger_liste_cis(fichier_cis)
        logger.info(f"CIS list loaded from file: {fichier_cis}")
    else:
        logger.info("Loading authorized CIS from database (Specialite.isBdm)...")
        cis_autorises = get_authorized_cis()

    if not cis_autorises:
        logger.error("No CIS codes loaded, stopping processing")
        return
    logger.info(f"{len(cis_autorises)} CIS codes loaded")

    # Get filename -> CIS mapping from database
    logger.info("Loading filename -> CIS mapping...")
    mapping = get_filename_to_cis_mapping()
    logger.info(f"{len(mapping)} mappings loaded")

    # Pre-filter: only keep filenames that map to authorized CIS codes
    # This avoids downloading files we'll skip anyway
    files_to_fetch = {
        filename: cis
        for filename, cis in mapping.items()
        if cis in cis_autorises and filename.startswith(pattern if pattern != "NR" else ("N", "R"))
    }
    logger.info(f"{len(files_to_fetch)} files match authorized CIS codes with pattern '{pattern}'")

    if not files_to_fetch:
        logger.warning("No files to process after filtering")
        return

    # List existing files in S3 to avoid NoSuchKey errors
    logger.info("Listing existing files in S3...")
    existing_keys = set(s3_client.list_html_files(pattern if pattern != "NR" else ""))
    existing_filenames = {key.split("/")[-1] for key in existing_keys}
    logger.info(f"{len(existing_filenames)} files exist in S3")

    # Filter to only files that exist in S3
    files_to_fetch = {f: cis for f, cis in files_to_fetch.items() if f in existing_filenames}
    logger.info(f"{len(files_to_fetch)} files to download after S3 existence check")

    if not files_to_fetch:
        logger.warning("No files to process after S3 existence check")
        return

    # Build full S3 keys from filenames
    html_keys = [f"{config.s3.html_prefix}{filename}" for filename in files_to_fetch.keys()]
    if limite is not None:
        html_keys = html_keys[:limite]
    logger.info(f"{len(html_keys)} files to download")

    # Download only the files we need
    logger.info("Downloading files...")
    fichiers_data = []
    for key in tqdm(html_keys, desc="Downloading", unit="file"):
        try:
            content = s3_client.download_file_content(key)
            fichiers_data.append((key, content, mapping, cis_autorises))
        except Exception as e:
            logger.error(f"Download error for {key}: {e}")

    # Process files
    logger.info("Processing files...")
    results = []
    files_processed = 0
    files_skipped = 0

    for fichier_data in tqdm(fichiers_data, desc="Processing", unit="file"):
        result = traiter_fichier_s3(fichier_data)
        if result is not None:
            results.append(result)
            files_processed += 1
        else:
            files_skipped += 1

    logger.info(f"Processing complete: {files_processed} processed, {files_skipped} skipped")

    # Write results to S3
    if results:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_key = f"{config.s3.output_prefix}parsed_{pattern}_{timestamp}.jsonl"

        output_content = "\n".join(json.dumps(r, ensure_ascii=False) for r in results)
        s3_client.upload_file_content(output_key, output_content, content_type="application/x-ndjson")

        logger.info(f"Results written to S3: {output_key}")
    else:
        logger.warning("No results to write")


def main():
    parser = argparse.ArgumentParser(
        description="Parse ANSM medication HTML documents (Notices and RCPs)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Local mode (uses database for CIS list by default)
  infomed-html-parser local ./html_files -o output.jsonl

  # Local mode with CIS file override
  infomed-html-parser local ./html_files --cis-file cis_list.txt -o output.jsonl

  # S3 mode (production on Scalingo)
  infomed-html-parser s3 --pattern N

Environment variables for S3 mode:
  S3_HOST       S3 endpoint URL
  S3_KEY_ID     S3 access key
  S3_KEY_SECRET S3 secret key
  S3_BUCKET_NAME          Bucket name (default: info-medicaments)
  S3_HTML_PREFIX          Prefix for HTML files (default: exports/html/)
  S3_OUTPUT_PREFIX        Prefix for output files (default: exports/parsed/)

Environment variables for database:
  MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE, MYSQL_PORT
        """,
    )

    subparsers = parser.add_subparsers(dest="command", help="Processing mode")

    # Local mode
    local_parser = subparsers.add_parser("local", help="Process local files")
    local_parser.add_argument("dossier_html", help="Folder containing HTML files")
    local_parser.add_argument("--cis-file", help="CIS file (default: uses database)")
    local_parser.add_argument("--output", "-o", default="output.jsonl", help="Output JSONL file")
    local_parser.add_argument("--limite", type=int, help="Limit number of files to process")
    local_parser.add_argument("--processes", type=int, default=None, help="Number of processes")
    local_parser.add_argument("--pattern", default="N", choices=["N", "R", "NR"], help="N=Notice, R=RCP, NR=both")

    # S3 mode
    s3_parser = subparsers.add_parser("s3", help="Process from S3 (Clever Cloud Cellar)")
    s3_parser.add_argument("--cis-file", help="CIS file (default: uses database)")
    s3_parser.add_argument("--limite", type=int, help="Limit number of files to process")
    s3_parser.add_argument("--pattern", default="N", choices=["N", "R", "NR"], help="N=Notice, R=RCP, NR=both")

    # Global options
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging")

    args = parser.parse_args()

    # Configure logging
    config = get_config()
    log_level = logging.DEBUG if args.verbose else getattr(logging, config.log_level, logging.INFO)
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    if args.command == "local":
        try:
            traiter_dossier_local(
                args.dossier_html,
                fichier_cis=args.cis_file,
                fichier_sortie=args.output,
                limite=args.limite,
                num_processes=args.processes,
                pattern=args.pattern,
            )
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "s3":
        try:
            traiter_depuis_s3(
                fichier_cis=args.cis_file,
                limite=args.limite,
                pattern=args.pattern,
            )
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    else:
        parser.print_help()
        raise SystemExit(1)


if __name__ == "__main__":
    mp.freeze_support()
    main()
