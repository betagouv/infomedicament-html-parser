"""Tests for the centralised EMA PDF parser (Abasaglar golden fixture)."""

from pathlib import Path

import pytest

from infomedicament_dataeng.centralise.extract import TextLine, _merge_same_line, _table_to_html
from infomedicament_dataeng.centralise.parser import parse_pdf

FIXTURES_DIR = Path(__file__).parent / "fixtures"
ABASAGLAR_PDF = FIXTURES_DIR / "abasaglar-epar-product-information_fr.pdf"


@pytest.fixture(scope="module")
def parsed() -> dict:
    return parse_pdf(ABASAGLAR_PDF.read_bytes())


def _headings(nodes, heading_type):
    return [n["content"] for n in nodes if n.get("type") == heading_type]


def _find(nodes, prefix, heading_type):
    return next(n for n in nodes if n.get("type") == heading_type and n["content"].startswith(prefix))


def _iter_all(nodes):
    for n in nodes:
        yield n
        if n.get("type") != "table":  # importer does not recurse into table children
            yield from _iter_all(n.get("children", []))


# ---------------------------------------------------------------------------
# Extraction helpers
# ---------------------------------------------------------------------------


class TestExtractHelpers:
    def test_merge_same_line_joins_number_and_title(self):
        lines = [
            TextLine("1.", 11.0, True, 0, 57.0, 69.0),
            TextLine("DÉNOMINATION DU MÉDICAMENT", 11.0, True, 0, 57.1, 69.0),
        ]
        merged = _merge_same_line(lines)
        assert len(merged) == 1
        assert merged[0].text == "1. DÉNOMINATION DU MÉDICAMENT"
        assert merged[0].bold is True

    def test_merge_keeps_distinct_y_separate(self):
        lines = [
            TextLine("first line", 11.0, False, 0, 100.0, 112.0),
            TextLine("next paragraph", 11.0, False, 0, 130.0, 142.0),
        ]
        assert len(_merge_same_line(lines)) == 2

    def test_table_to_html_escapes_and_wraps(self):
        html = _table_to_html([["a & b", "c"], ["d", None]])
        assert html.startswith("<table><tbody>")
        assert "<td>a &amp; b</td>" in html
        assert "<td></td>" in html  # None cell


# ---------------------------------------------------------------------------
# Annexe segmentation (golden)
# ---------------------------------------------------------------------------


class TestAnnexeSplit:
    def test_rcp_has_ten_top_level_sections(self, parsed):
        headings = _headings(parsed["rcp"], "AmmAnnexeTitre1")
        numbers = [h.split(".")[0] for h in headings]
        assert numbers == [str(i) for i in range(1, 11)]

    def test_notice_has_six_sections(self, parsed):
        headings = _headings(parsed["notice"], "AmmNoticeTitre1")
        numbers = [h.split(".")[0] for h in headings]
        assert numbers == [str(i) for i in range(1, 7)]

    def test_rcp_section4_has_subsections_4_1_to_4_9(self, parsed):
        section4 = _find(parsed["rcp"], "4.", "AmmAnnexeTitre1")
        subs = _headings(section4["children"], "AmmAnnexeTitre2")
        assert [s.split()[0] for s in subs] == [f"4.{i}" for i in range(1, 10)]


# ---------------------------------------------------------------------------
# Anchor mapping
# ---------------------------------------------------------------------------


class TestAnchors:
    def test_rcp_subsection_anchors(self, parsed):
        section4 = _find(parsed["rcp"], "4.", "AmmAnnexeTitre1")
        anchors = {s["content"].split()[0]: s["anchor"] for s in section4["children"]}
        assert anchors["4.1"] == "RcpIndicTherap"
        assert anchors["4.8"] == "RcpEffetsIndesirables"

    def test_notice_section_anchors(self, parsed):
        anchors = {
            n["content"].split(".")[0]: n["anchor"] for n in parsed["notice"] if n.get("type") == "AmmNoticeTitre1"
        }
        assert anchors["1"] == "Ann3bQuestceque"
        assert anchors["2"] == "Ann3bInfoNecessaires"
        assert anchors["4"] == "Ann3bEffetsIndesirables"


# ---------------------------------------------------------------------------
# Metadata + tables + contract
# ---------------------------------------------------------------------------


class TestMetadataAndTables:
    def test_rcp_title_is_denomination(self, parsed):
        first = parsed["rcp"][0]
        assert first["type"] == "AmmAnnexeTitre"
        assert "ABASAGLAR" in first["content"]

    def test_table_rendered_as_html_in_section_4_8(self, parsed):
        section4 = _find(parsed["rcp"], "4.", "AmmAnnexeTitre1")
        s48 = _find(section4["children"], "4.8", "AmmAnnexeTitre2")
        tables = [c for c in s48["children"] if c.get("type") == "table"]
        assert tables
        assert tables[0]["html"].startswith("<table>")
        assert tables[0]["children"]  # truthy so the importer keeps it


class TestImporterContract:
    def test_no_node_violates_insertion_guard(self, parsed):
        for kind in ("rcp", "notice"):
            for node in _iter_all(parsed[kind]):
                has_payload = node.get("content") or node.get("children") or node.get("text")
                assert has_payload, f"empty node in {kind}: {node.get('type')}"

    def test_tables_carry_non_empty_html(self, parsed):
        for kind in ("rcp", "notice"):
            for node in _iter_all(parsed[kind]):
                if node.get("type") == "table":
                    assert node.get("html")
