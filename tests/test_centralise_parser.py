"""Tests for the centralised EMA PDF parser (Abasaglar golden fixture)."""

from pathlib import Path

import pytest

from infomedicament_dataeng.centralise.extract import TextLine, _merge_same_line, _table_to_html
from infomedicament_dataeng.centralise.match import match_presentation
from infomedicament_dataeng.centralise.parser import _is_notice_header, assemble_document, parse_pdf

FIXTURES_DIR = Path(__file__).parent / "fixtures"
ABASAGLAR_PDF = FIXTURES_DIR / "abasaglar-epar-product-information_fr.pdf"

# Real PDBM SpecDenom01 for the three CIS sharing the Abasaglar PDF.
CIS_NAMES = {
    "cartouche": "ABASAGLAR 100 unités/ml, solution injectable en cartouche",
    "generic_pen": "ABASAGLAR 100 unités/ml, solution injectable en stylo prérempli",
    "tempo_pen": "ABASAGLAR 100 unités/mL TEMPO PEN, solution injectable en stylo prérempli",
}


@pytest.fixture(scope="module")
def parsed() -> dict:
    return parse_pdf(ABASAGLAR_PDF.read_bytes())


@pytest.fixture(scope="module")
def rcp(parsed) -> list:
    return parsed["rcp"][0]["content"]  # cartouche SmPC is first


@pytest.fixture(scope="module")
def notice(parsed) -> list:
    return parsed["notice"][0]["content"]  # cartouche notice is first


def _headings(nodes, heading_type):
    return [n["content"] for n in nodes if n.get("type") == heading_type]


def _find(nodes, prefix, heading_type):
    return next(n for n in nodes if n.get("type") == heading_type and n["content"].startswith(prefix))


def _iter_all(nodes):
    for n in nodes:
        yield n
        if n.get("type") != "table":  # mirror the importer's is_table guard (only literal "table" is skipped)
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
# Presentations (one SmPC/Notice per device)
# ---------------------------------------------------------------------------


class TestPresentations:
    def test_rcp_has_cartouche_and_pen_smpcs(self, parsed):
        denoms = [d["denomination"].lower() for d in parsed["rcp"]]
        assert len(denoms) == 2
        assert any("cartouche" in d for d in denoms)
        assert any("stylo prérempli" in d for d in denoms)

    def test_notice_has_three_device_presentations(self, parsed):
        denoms = [d["denomination"].lower() for d in parsed["notice"]]
        assert len(denoms) == 3
        assert any("cartouche" in d for d in denoms)
        assert any("kwikpen" in d for d in denoms)
        assert any("tempo" in d for d in denoms)


# ---------------------------------------------------------------------------
# Presentation matching (Jaccard)
# ---------------------------------------------------------------------------


class TestMatching:
    def test_each_cis_matches_its_notice(self, parsed):
        notices = parsed["notice"]
        assert "cartouche" in match_presentation(CIS_NAMES["cartouche"], notices)["denomination"].lower()
        # generic "stylo prérempli" resolves to KwikPen, not Tempo Pen
        assert "kwikpen" in match_presentation(CIS_NAMES["generic_pen"], notices)["denomination"].lower()
        assert "tempo" in match_presentation(CIS_NAMES["tempo_pen"], notices)["denomination"].lower()

    def test_cartouche_cis_matches_cartouche_smpc(self, parsed):
        matched = match_presentation(CIS_NAMES["cartouche"], parsed["rcp"])
        assert "cartouche" in matched["denomination"].lower()

    def test_dosage_matching_prefers_exact_strength(self):
        # Brintellix: 20 mg tablet must beat "20 mg/ml solution" despite the shared "20"
        docs = [
            {"denomination": "Brintellix 20 mg comprimé pelliculé"},
            {"denomination": "Brintellix 20 mg/ml solution buvable en gouttes"},
        ]
        matched = match_presentation("BRINTELLIX 20 mg, comprimé pelliculé", docs)
        assert "comprimé" in matched["denomination"]

    def test_single_doc_returned_unconditionally(self):
        only = {"denomination": "whatever", "content": [], "date_notif": ""}
        assert match_presentation("unrelated name", [only]) is only

    def test_empty_name_with_multiple_presentations_skips(self):
        docs = [{"denomination": "5 mg comprimé"}, {"denomination": "10 mg comprimé"}]
        assert match_presentation("", docs) is None

    def test_empty_docs_returns_none(self):
        assert match_presentation("x", []) is None


class TestNoticeHeaderDetection:
    def test_matches_both_qrd_wordings(self):
        assert _is_notice_header("Notice: Information de l’utilisateur")  # Abasaglar
        assert _is_notice_header("Notice : Information du patient")  # Brintellix

    def test_rejects_non_headers(self):
        assert not _is_notice_header("Que contient cette notice?")
        assert not _is_notice_header("Gardez cette notice.")


# ---------------------------------------------------------------------------
# Annexe segmentation (golden, on the cartouche presentation)
# ---------------------------------------------------------------------------


class TestAnnexeSplit:
    def test_rcp_has_ten_top_level_sections(self, rcp):
        numbers = [h.split(".")[0] for h in _headings(rcp, "AmmAnnexeTitre1")]
        assert numbers == [str(i) for i in range(1, 11)]

    def test_notice_has_six_sections(self, notice):
        numbers = [h.split(".")[0] for h in _headings(notice, "AmmNoticeTitre1")]
        assert numbers == [str(i) for i in range(1, 7)]

    def test_rcp_section4_has_subsections_4_1_to_4_9(self, rcp):
        section4 = _find(rcp, "4.", "AmmAnnexeTitre1")
        subs = _headings(section4["children"], "AmmAnnexeTitre2")
        assert [s.split()[0] for s in subs] == [f"4.{i}" for i in range(1, 10)]


# ---------------------------------------------------------------------------
# Anchor mapping
# ---------------------------------------------------------------------------


class TestAnchors:
    def test_rcp_subsection_anchors(self, rcp):
        section4 = _find(rcp, "4.", "AmmAnnexeTitre1")
        anchors = {s["content"].split()[0]: s["anchor"] for s in section4["children"]}
        assert anchors["4.1"] == "RcpIndicTherap"
        assert anchors["4.8"] == "RcpEffetsIndesirables"

    def test_notice_section_anchors(self, notice):
        anchors = {n["content"].split(".")[0]: n["anchor"] for n in notice if n.get("type") == "AmmNoticeTitre1"}
        assert anchors["1"] == "Ann3bQuestceque"
        assert anchors["2"] == "Ann3bInfoNecessaires"
        assert anchors["4"] == "Ann3bEffetsIndesirables"


# ---------------------------------------------------------------------------
# Metadata + tables + contract
# ---------------------------------------------------------------------------


class TestMetadataAndTables:
    def test_rcp_denomination_is_cartouche(self, parsed):
        assert "cartouche" in parsed["rcp"][0]["denomination"].lower()

    def test_assemble_document_prepends_title(self, rcp):
        content = assemble_document(rcp, "MY TITLE", "07/2020")
        assert content[0] == {"type": "AmmAnnexeTitre", "content": "MY TITLE"}
        assert content[1] == {"type": "DateNotif", "content": "07/2020"}

    def test_table_rendered_in_section_4_8(self, rcp):
        section4 = _find(rcp, "4.", "AmmAnnexeTitre1")
        s48 = _find(section4["children"], "4.8", "AmmAnnexeTitre2")
        tables = [c for c in s48["children"] if c.get("type") == "AmmCorpsTexteTable"]
        assert tables
        # The frontend renders from the children tree: tr -> td, cell text in `content`.
        rows = tables[0]["children"]
        assert rows and rows[0]["tag"] == "tr"
        cells = rows[0]["children"]
        assert cells and cells[0]["tag"] == "td"
        assert isinstance(cells[0]["content"], list) and cells[0]["content"][0]


class TestImporterContract:
    def test_no_node_violates_insertion_guard(self, parsed):
        for kind in ("rcp", "notice"):
            for doc in parsed[kind]:
                content = assemble_document(doc["content"], doc["denomination"], doc["date_notif"])
                for node in _iter_all(content):
                    has_payload = node.get("content") or node.get("children") or node.get("text")
                    assert has_payload, f"empty node in {kind}: {node.get('type')}"

    def test_tables_carry_non_empty_html(self, parsed):
        for kind in ("rcp", "notice"):
            for doc in parsed[kind]:
                for node in _iter_all(doc["content"]):
                    if node.get("type") == "AmmCorpsTexteTable":
                        assert node.get("html")
