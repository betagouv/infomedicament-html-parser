"""Tests for the centralised EMA PDF parser (Abasaglar golden fixture)."""

from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from infomedicament_dataeng.centralise.extract import TextLine, _merge_same_line, _table_to_html
from infomedicament_dataeng.centralise.match import match_presentation
from infomedicament_dataeng.centralise.parser import (
    _is_notice_header,
    _normalise_date,
    _SemanticHtmlBuilder,
    parse_pdf,
)

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
def rcp(parsed) -> BeautifulSoup:
    return BeautifulSoup(parsed["rcp"][0]["content_html"], "html.parser")  # cartouche SmPC is first


@pytest.fixture(scope="module")
def notice(parsed) -> BeautifulSoup:
    return BeautifulSoup(parsed["notice"][0]["content_html"], "html.parser")  # cartouche notice is first


def _headings(document, tag):
    return [heading.get_text(" ", strip=True) for heading in document.find_all(tag)]


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

    def test_pdf_builder_constructs_semantic_elements_directly(self):
        builder = _SemanticHtmlBuilder("rcp", "https://cdn.example.test/images", "images/", {})
        builder.add(TextLine("1. DÉNOMINATION DU MÉDICAMENT", 11, True, 0, 10, 20))
        builder.add(TextLine("Example medicine", 11, False, 0, 30, 40))
        builder.add(TextLine("- First item", 11, False, 0, 50, 60))
        builder.add(TextLine("2. COMPOSITION QUALITATIVE ET QUANTITATIVE", 11, True, 0, 70, 80))

        fragment = builder.finish()

        assert fragment.startswith('<h2 id="RcpDenomination">1. DÉNOMINATION DU MÉDICAMENT</h2>')
        assert "<p>Example medicine</p>" in fragment
        assert "<ul><li>First item</li></ul>" in fragment
        assert 'class="' not in fragment


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
        only = {"denomination": "whatever", "content_html": "", "date_notif": None}
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
        numbers = [h.split(".")[0] for h in _headings(rcp, "h2")]
        assert numbers == [str(i) for i in range(1, 11)]

    def test_notice_has_six_sections(self, notice):
        numbers = [h.split(".")[0] for h in _headings(notice, "h3")]
        assert numbers == [str(i) for i in range(1, 7)]

    def test_rcp_section4_has_subsections_4_1_to_4_9(self, rcp):
        subs = [heading for heading in _headings(rcp, "h3") if heading.startswith("4.")]
        assert [s.split()[0] for s in subs] == [f"4.{i}" for i in range(1, 10)]


# ---------------------------------------------------------------------------
# Anchor mapping
# ---------------------------------------------------------------------------


class TestAnchors:
    def test_rcp_subsection_anchors(self, rcp):
        anchors = {
            heading.get_text(" ", strip=True).split()[0]: heading["id"]
            for heading in rcp.find_all("h3")
            if heading.get_text(strip=True).startswith("4.")
        }
        assert anchors["4.1"] == "RcpIndicTherap"
        assert anchors["4.8"] == "RcpEffetsIndesirables"

    def test_notice_section_anchors(self, notice):
        anchors = {heading.get_text(strip=True).split(".")[0]: heading["id"] for heading in notice.find_all("h3")}
        assert anchors["1"] == "Ann3bQuestceque"
        assert anchors["2"] == "Ann3bInfoNecessaires"
        assert anchors["4"] == "Ann3bEffetsIndesirables"


# ---------------------------------------------------------------------------
# Metadata + tables + contract
# ---------------------------------------------------------------------------


class TestSemanticContract:
    def test_rcp_denomination_is_cartouche(self, parsed):
        assert "cartouche" in parsed["rcp"][0]["denomination"].lower()

    def test_records_expose_semantic_html_not_content_trees(self, parsed):
        for kind in ("rcp", "notice"):
            for document in parsed[kind]:
                assert "content" not in document
                assert document["content_html"].startswith("<")
                assert 'data-block-id="document-b0001"' in document["content_html"]
                assert 'class="' not in document["content_html"]

    def test_notice_has_document_heading_above_numbered_sections(self, notice):
        assert notice.h2.get_text(" ", strip=True).startswith("Notice:")
        assert notice.h3.get_text(" ", strip=True).startswith("1.")

    def test_table_rendered_in_section_4_8(self, rcp):
        heading = rcp.find("h3", id="RcpEffetsIndesirables")
        assert heading is not None
        table = heading.find_next("table")
        assert table is not None
        assert table.find("td").get_text(strip=True)

    def test_notice_indication_is_extracted_and_marked(self, parsed):
        document = parsed["notice"][0]
        assert document["indication"]
        html = BeautifulSoup(document["content_html"], "html.parser")
        assert html.find(attrs={"data-document-role": "indication"})

    def test_glossary_terms_use_the_shared_semantic_annotation(self):
        result = parse_pdf(ABASAGLAR_PDF.read_bytes(), glossary_terms=["insuline"])
        assert '<span data-definition="insuline">insuline</span>' in result["rcp"][0]["content_html"]

    def test_complete_dates_are_normalised_without_inventing_partial_dates(self):
        assert _normalise_date("09 septembre 2014") == "2014-09-09"
        assert _normalise_date("09/09/2014") == "2014-09-09"
        assert _normalise_date("07/2020") is None

    def test_mixed_inline_bold_is_preserved(self, notice):
        emphasis = notice.find("strong", string="Prenez contact avec un médecin rapidement")

        assert emphasis is not None
        assert emphasis.parent.name in {"p", "li"}
        assert "Dans la plupart des cas" in emphasis.parent.get_text()

    def test_pdf_underlines_are_preserved(self, rcp):
        underline = rcp.find("u", string="Posologie")

        assert underline is not None
        assert underline.parent.name == "p"


def _image_nodes(parsed):
    return [
        image
        for kind in ("rcp", "notice")
        for document in parsed[kind]
        for image in BeautifulSoup(document["content_html"], "html.parser").find_all("img")
    ]


class TestImages:
    def test_blobs_extracted_and_content_addressed(self, parsed):
        images = parsed["images"]
        assert images  # the Abasaglar notices carry injection diagrams
        for key in images:
            assert key.startswith("exports/images/centralise/")  # default image_prefix

    def test_image_nodes_reference_uploaded_blobs(self, parsed):
        nodes = _image_nodes(parsed)
        assert nodes
        keys = parsed["images"]
        for image in nodes:
            assert image.parent.name == "figure"
            sha = image["src"].split("/centralise/")[1].rsplit(".", 1)[0]
            assert any(sha in k for k in keys), "image URL must point at an uploaded blob"
