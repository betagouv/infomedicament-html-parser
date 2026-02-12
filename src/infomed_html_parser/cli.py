"""Command-line interface for the HTML parser."""

import argparse
import csv
import glob
import json
import logging
import multiprocessing as mp
import os
from datetime import datetime
from pathlib import Path

import chardet
from tqdm import tqdm

from .config import get_config
from .db import get_authorized_cis, get_filename_to_cis_mapping
from .io import charger_liste_cis
from .parser import html_vers_json
from .s3 import S3Client
from .sql_to_csv import sql_to_csv

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
        pattern: File pattern to process ("N" for Notices, "R" for RCP)
    """
    if num_processes is None:
        num_processes = mp.cpu_count()

    logger.info(f"Local mode - {num_processes} processes")

    # Build glob pattern
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
    fichier_sortie: str | None = None,
    limite: int | None = None,
    pattern: str = "N",
    batch_size: int = 500,
) -> None:
    """
    Process HTML files from S3 and write results to S3 or locally.

    Args:
        fichier_cis: Local file containing authorized CIS codes (if None, uses database)
        fichier_sortie: Local output JSONL file (if None, writes to S3)
        limite: Limit number of files to process (for testing)
        pattern: File pattern to process ("N" for Notices, "R" for RCP)
        batch_size: Number of files to process per batch (to limit memory usage)
    """
    config = get_config()

    if not config.s3.is_configured():
        raise RuntimeError("S3 credentials not configured. Set S3_KEY_ID and S3_KEY_SECRET.")

    s3_client = S3Client(config.s3)

    logger.info("S3 mode - Clever Cloud Cellar")
    logger.info(f"Bucket: {config.s3.bucket_name}")
    html_prefix = config.s3.notice_prefix if pattern == "N" else config.s3.rcp_prefix
    logger.info(f"HTML prefix: {html_prefix}")

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
        if cis in cis_autorises and filename.startswith(pattern)
    }
    logger.info(f"{len(files_to_fetch)} files match authorized CIS codes with pattern '{pattern}'")

    if not files_to_fetch:
        logger.warning("No files to process after filtering")
        return

    # List existing files in S3 to avoid NoSuchKey errors
    logger.info("Listing existing files in S3...")
    existing_keys = set(s3_client.list_html_files(pattern))
    existing_filenames = {key.split("/")[-1] for key in existing_keys}
    logger.info(f"{len(existing_filenames)} files exist in S3")

    # Filter to only files that exist in S3
    files_to_fetch = {f: cis for f, cis in files_to_fetch.items() if f in existing_filenames}
    logger.info(f"{len(files_to_fetch)} files to download after S3 existence check")

    if not files_to_fetch:
        logger.warning("No files to process after S3 existence check")
        return

    # Build full S3 keys from filenames
    html_keys = [f"{html_prefix}{filename}" for filename in files_to_fetch.keys()]
    if limite is not None:
        html_keys = html_keys[:limite]

    total_files = len(html_keys)
    num_batches = (total_files + batch_size - 1) // batch_size
    logger.info(f"{total_files} files to process in {num_batches} batches of {batch_size}")

    # If writing locally, initialize the output file
    if fichier_sortie:
        with open(fichier_sortie, "w", encoding="utf-8") as f_out:
            pass
        logger.info(f"Local output: {fichier_sortie}")

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    total_processed = 0
    total_skipped = 0

    for batch_num in range(num_batches):
        batch_start = batch_num * batch_size
        batch_end = min(batch_start + batch_size, total_files)
        batch_keys = html_keys[batch_start:batch_end]

        logger.info(f"Batch {batch_num + 1}/{num_batches}: processing files {batch_start + 1}-{batch_end}")

        # Download batch
        batch_results = []
        for key in tqdm(batch_keys, desc=f"Batch {batch_num + 1}", unit="file"):
            try:
                content = s3_client.download_file_content(key)
                result = traiter_fichier_s3((key, content, mapping, cis_autorises))
                if result is not None:
                    batch_results.append(result)
                    total_processed += 1
                else:
                    total_skipped += 1
            except Exception as e:
                logger.error(f"Error processing {key}: {e}")
                total_skipped += 1

        # Write batch results
        if batch_results:
            if fichier_sortie:
                with open(fichier_sortie, "a", encoding="utf-8") as f_out:
                    for r in batch_results:
                        f_out.write(json.dumps(r, ensure_ascii=False) + "\n")
                logger.info(f"Batch {batch_num + 1} appended to {fichier_sortie} ({len(batch_results)} results)")
            else:
                output_key = f"{config.s3.output_prefix}parsed_{pattern}_{timestamp}_batch{batch_num + 1:03d}.jsonl"
                output_content = "\n".join(json.dumps(r, ensure_ascii=False) for r in batch_results)
                s3_client.upload_file_content(output_key, output_content, content_type="application/x-ndjson")
                logger.info(f"Batch {batch_num + 1} written to S3: {output_key} ({len(batch_results)} results)")

    logger.info(f"Processing complete: {total_processed} processed, {total_skipped} skipped")


def run_pediatric_classification(
    rcp_path: str,
    truth_path: str | None,
    output_path: str,
) -> None:
    """Run pediatric classification on parsed RCPs and optionally evaluate."""
    from .pediatric import (
        classify,
        compute_metrics,
        format_metrics,
        load_ground_truth,
    )

    # Load ground truth (for ATC codes and evaluation)
    ground_truth = {}
    if truth_path:
        ground_truth = load_ground_truth(truth_path)
        logger.info(f"Ground truth loaded: {len(ground_truth)} entries")

    # Load and classify each RCP
    predictions = []
    with open(rcp_path, encoding="utf-8") as f:
        for line in f:
            rcp_json = json.loads(line)
            source = rcp_json.get("source", {})
            cis = source.get("cis", "") if isinstance(source, dict) else ""
            atc_code = ground_truth.get(cis, {}).get("atc", "")
            pred = classify(rcp_json, atc_code=atc_code)
            predictions.append(pred)

    logger.info(f"Classified {len(predictions)} drugs")

    # Write predictions CSV
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        header = ["cis", "pred_A", "pred_B", "pred_C"]
        if ground_truth:
            header += ["truth_A", "truth_B", "truth_C", "match_A", "match_B", "match_C"]
        header += ["c_reasons", "keywords_41_42", "keywords_43", "evidence_41_42", "evidence_43"]
        writer.writerow(header)

        for pred in predictions:
            gt = ground_truth.get(pred.cis, {})
            row = [
                pred.cis,
                int(pred.condition_a),
                int(pred.condition_b),
                int(pred.condition_c),
            ]
            if ground_truth:
                truth_a = gt.get("A", "")
                truth_b = gt.get("B", "")
                truth_c = gt.get("C", "")
                row += [
                    int(truth_a) if isinstance(truth_a, bool) else "",
                    int(truth_b) if isinstance(truth_b, bool) else "",
                    int(truth_c) if isinstance(truth_c, bool) else "",
                    int(pred.condition_a == truth_a) if isinstance(truth_a, bool) else "",
                    int(pred.condition_b == truth_b) if isinstance(truth_b, bool) else "",
                    int(pred.condition_c == truth_c) if isinstance(truth_c, bool) else "",
                ]
            # Explainability columns
            kw_41_42 = []
            for m in pred.matches_41_42:
                kw_41_42.extend(m.keywords)
            kw_43 = []
            for m in pred.matches_43:
                kw_43.extend(m.keywords)
            row += [
                " | ".join(pred.c_reasons),
                " | ".join(dict.fromkeys(kw_41_42)),
                " | ".join(dict.fromkeys(kw_43)),
                " ||| ".join(m.text[:200] for m in pred.matches_41_42),
                " ||| ".join(m.text[:200] for m in pred.matches_43),
            ]
            writer.writerow(row)

    logger.info(f"Predictions written to {output_path}")

    # Evaluate if ground truth provided
    if ground_truth:
        metrics = compute_metrics(predictions, ground_truth)
        print(format_metrics(metrics))


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
    local_parser.add_argument("--pattern", default="N", choices=["N", "R"], help="N=Notice, R=RCP")

    # S3 mode
    s3_parser = subparsers.add_parser("s3", help="Process from S3 (Clever Cloud Cellar)")
    s3_parser.add_argument("--cis-file", help="CIS file (default: uses database)")
    s3_parser.add_argument("--output", "-o", help="Local output JSONL file (default: writes to S3)")
    s3_parser.add_argument("--limite", type=int, help="Limit number of files to process")
    s3_parser.add_argument("--pattern", default="N", choices=["N", "R"], help="N=Notice, R=RCP")
    s3_parser.add_argument("--batch-size", type=int, default=500, help="Files per batch (default: 500)")

    # SQL to CSV mode
    sql_parser = subparsers.add_parser("sql-to-csv", help="Convert SQL INSERT statements to CSV")
    sql_parser.add_argument("sql_file", help="SQL file to convert")
    sql_parser.add_argument("--output", "-o", help="Output CSV file (default: same name with .csv)")
    sql_parser.add_argument("--encoding", "-e", default="iso-8859-1", help="Source file encoding")
    sql_parser.add_argument("--dialect", "-d", default="tsql", help="SQL dialect (tsql, mysql, postgres)")

    # Pediatric classification mode
    ped_parser = subparsers.add_parser("classify-pediatric", help="Classify drugs for pediatric use")
    ped_parser.add_argument("--rcp", required=True, help="Parsed RCP JSONL file")
    ped_parser.add_argument("--truth", help="Ground truth CSV (for evaluation)")
    ped_parser.add_argument("--output", "-o", default="data/predictions.csv", help="Output predictions CSV")

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
                fichier_sortie=args.output,
                limite=args.limite,
                pattern=args.pattern,
                batch_size=args.batch_size,
            )
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "sql-to-csv":
        try:
            output_path = Path(args.output) if args.output else None
            sql_to_csv(Path(args.sql_file), output_path, args.encoding, args.dialect)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    elif args.command == "classify-pediatric":
        try:
            run_pediatric_classification(args.rcp, args.truth, args.output)
        except Exception as e:
            logger.exception(f"Error: {e}")
            raise SystemExit(1)

    else:
        parser.print_help()
        raise SystemExit(1)


if __name__ == "__main__":
    mp.freeze_support()
    main()
