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


def _doc(denom, tag):
    return {"denomination": denom, "date_notif": "", "content": [{"type": "AmmCorpsTexte", "content": tag}]}


class TestParseFanOut:
    """`centralise parse` emits one line per CIS, each matched to its presentation."""

    def test_fans_out_and_matches_presentation(self, tmp_path, monkeypatch):
        import glob
        import json

        from infomedicament_dataeng import cli, db
        from infomedicament_dataeng.centralise import acquire as acq
        from infomedicament_dataeng.centralise import parser

        worklist = {
            URL: [
                ("111", "ABASAGLAR 100 unités/ml solution injectable en cartouche"),
                ("222", "ABASAGLAR 100 unités/ml solution injectable en stylo prérempli"),
            ]
        }
        parsed = {
            "rcp": [_doc("… en cartouche", "rcp-cartouche"), _doc("… KwikPen … stylo prérempli", "rcp-pen")],
            "notice": [_doc("… en cartouche", "notice-cartouche"), _doc("… KwikPen … stylo prérempli", "notice-pen")],
        }

        monkeypatch.setattr(cli, "make_s3_client", lambda: object())
        monkeypatch.setattr(acq, "get_ema_pdf", lambda url, s3=None, **k: b"%PDF")
        monkeypatch.setattr(db, "get_centralised_worklist", lambda cis=None: worklist)
        monkeypatch.setattr(parser, "parse_pdf", lambda b: parsed)

        cli.run_centralise_parse(output_dir=str(tmp_path))

        files = glob.glob(str(tmp_path / "parsed_R_*.jsonl"))
        assert len(files) == 1
        by_cis = {json.loads(ln)["source"]["cis"]: json.loads(ln) for ln in open(files[0]).read().splitlines()}
        assert set(by_cis) == {"111", "222"}
        # each CIS got the content of its own presentation
        assert by_cis["111"]["content"][-1]["content"] == "rcp-cartouche"
        assert by_cis["222"]["content"][-1]["content"] == "rcp-pen"
        # title is the CIS's own SpecDenom01
        assert by_cis["111"]["content"][0] == {"type": "AmmAnnexeTitre", "content": worklist[URL][0][1]}
