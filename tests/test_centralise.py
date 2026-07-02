"""Tests for the centralised EMA PDF acquisition (S3 cache)."""

import pytest

from infomedicament_dataeng.centralise import acquire
from infomedicament_dataeng.centralise.acquire import get_ema_pdf, pdf_cache_key

URL = "https://www.ema.europa.eu/fr/documents/product-information/abasaglar-epar-product-information_fr.pdf"
EXPECTED_KEY = "imports/ema_pdf/abasaglar-epar-product-information_fr.pdf"


class FakeS3Client:
    """Minimal fake S3Client tracking uploads/downloads for the cache tests."""

    def __init__(self, exists=False, stored=b""):
        self._exists = exists
        self._stored = stored
        self.uploads = {}  # key -> content
        self.downloaded = []

    def object_exists(self, key):
        return self._exists

    def download_file_content(self, key):
        self.downloaded.append(key)
        return self._stored

    def upload_file_content(self, key, content, content_type="application/json"):
        self.uploads[key] = content


class TestPdfCacheKey:
    def test_from_url_slug(self):
        assert pdf_cache_key(URL) == EXPECTED_KEY

    def test_rejects_empty_slug(self):
        with pytest.raises(ValueError):
            pdf_cache_key("")


class TestGetEmaPdf:
    def test_cache_hit_serves_from_s3(self, monkeypatch):
        def _boom(url):
            raise AssertionError("should not fetch from EMA on cache hit")

        monkeypatch.setattr(acquire, "_fetch_from_ema", _boom)
        s3 = FakeS3Client(exists=True, stored=b"%PDF-cached")

        assert get_ema_pdf(URL, s3) == b"%PDF-cached"
        assert s3.downloaded == [EXPECTED_KEY]
        assert s3.uploads == {}

    def test_cache_miss_fetches_and_uploads_with_sidecar(self, monkeypatch):
        monkeypatch.setattr(acquire, "_fetch_from_ema", lambda url: b"%PDF-fresh")
        s3 = FakeS3Client(exists=False)

        assert get_ema_pdf(URL, s3) == b"%PDF-fresh"
        assert set(s3.uploads) == {EXPECTED_KEY, f"{EXPECTED_KEY}.sha256"}
        assert s3.uploads[EXPECTED_KEY] == b"%PDF-fresh"

    def test_refresh_bypasses_cache(self, monkeypatch):
        monkeypatch.setattr(acquire, "_fetch_from_ema", lambda url: b"%PDF-new")
        s3 = FakeS3Client(exists=True, stored=b"%PDF-old")

        assert get_ema_pdf(URL, s3, refresh=True) == b"%PDF-new"
        assert s3.downloaded == []
        assert EXPECTED_KEY in s3.uploads
