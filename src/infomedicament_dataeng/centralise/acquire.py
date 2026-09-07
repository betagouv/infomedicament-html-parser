"""Acquire EMA product-information PDFs, cached on S3 to avoid re-scraping EMA."""

import hashlib
import logging
import time
import urllib.error
import urllib.request
from collections.abc import Callable

from ..config import get_config
from ..s3 import S3Client, make_s3_client

logger = logging.getLogger(__name__)

# EMA serves the PDF fine with a plain UA; set one to avoid default-urllib blocks.
_USER_AGENT = "infomedicament-dataeng/0.1 (+https://info-medicaments.fr)"

# EMA rate-limits scraping (HTTP 429). Retry with exponential backoff, honoring
# the Retry-After header when present.
_MAX_RETRIES = 5
_BACKOFF_BASE = 5.0  # seconds; doubled each retry


def _retry_after_seconds(err: urllib.error.HTTPError, attempt: int) -> float:
    """How long to wait before the next retry.

    Always at least the exponential backoff; a Retry-After header is honored only
    when it asks for *longer* (EMA sends ``Retry-After: 0``, which is useless).
    """
    backoff = _BACKOFF_BASE * (2**attempt)
    header = err.headers.get("Retry-After") if err.headers else None
    if header:
        try:
            return max(float(header), backoff)
        except ValueError:
            pass  # HTTP-date form is rare here; fall through to backoff
    return backoff


def pdf_cache_key(url: str) -> str:
    """S3 key for a cached EMA PDF, derived from the URL's filename slug.

    e.g. ``.../abasaglar-epar-product-information_fr.pdf`` →
    ``imports/ema_pdf/abasaglar-epar-product-information_fr.pdf``. The slug is
    stable and dedups the many-CIS-share-one-PDF case.
    """
    slug = url.rstrip("/").split("/")[-1]
    if not slug:
        raise ValueError(f"Cannot derive a cache key from URL: {url!r}")
    return f"{get_config().s3.ema_pdf_prefix}{slug}"


def _fetch_from_ema(url: str) -> bytes:
    logger.info(f"Fetching PDF from EMA: {url}")
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    for attempt in range(_MAX_RETRIES):
        try:
            with urllib.request.urlopen(req) as response:
                return response.read()
        except urllib.error.HTTPError as err:
            if err.code != 429 or attempt == _MAX_RETRIES - 1:
                raise
            wait = _retry_after_seconds(err, attempt)
            logger.warning(f"EMA returned 429; retrying in {wait:.0f}s ({attempt + 1}/{_MAX_RETRIES}): {url}")
            time.sleep(wait)
    raise RuntimeError("unreachable")  # loop either returns or raises


def get_ema_pdf(
    url: str,
    s3_client: S3Client | None = None,
    *,
    refresh: bool = False,
    on_cache_hit: Callable[[str], None] | None = None,
) -> bytes:
    """Return the PDF bytes for an EMA PI URL, using the S3 cache.

    Serves from S3 when the PDF is already cached, unless ``refresh`` is set. On
    a cache miss (or refresh) fetches from EMA, uploads the PDF plus a
    ``.sha256`` sidecar (for parse-step idempotency), and returns the bytes.

    EMA is hit at most once per distinct PDF, ever, unless ``refresh`` is passed.
    ``on_cache_hit`` can update an interactive display without emitting a log
    line for every cached PDF.
    """
    if s3_client is None:
        s3_client = make_s3_client()

    key = pdf_cache_key(url)

    if not refresh and s3_client.object_exists(key):
        logger.debug(f"Cache hit: {key}")
        if on_cache_hit is not None:
            on_cache_hit(key)
        return s3_client.download_file_content(key)

    pdf_bytes = _fetch_from_ema(url)
    s3_client.upload_file_content(key, pdf_bytes, content_type="application/pdf")
    digest = hashlib.sha256(pdf_bytes).hexdigest()
    s3_client.upload_file_content(f"{key}.sha256", digest, content_type="text/plain")
    logger.info(f"Cached {len(pdf_bytes)} bytes to {key} (sha256={digest[:12]}…)")
    return pdf_bytes
