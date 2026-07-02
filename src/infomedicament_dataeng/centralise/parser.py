"""Segment an EMA QRD-template PDF into RCP + Notice content node-lists.

Emits the same node shapes the importer consumes (see the plan / ``db.py``):
``AmmAnnexeTitre``/``DateNotif`` metadata, ``AmmAnnexeTitre1/2`` &
``AmmNoticeTitre1`` headings, ``AmmCorpsTexte``/``AmmCorpsTexteGras`` bodies,
``listePuce`` bullets, and ``table`` nodes. Anchors are looked up from the
authoritative maps in ``opensearch.sections`` (no duplication).
"""

import html
import re

import fitz

from ..opensearch.sections import _NOTICE_NUMBER_TO_ANCHOR, _RCP_NUMBER_TO_ANCHOR
from .extract import Table, TextLine, extract_elements

# Heading patterns (applied only to bold lines). L2 is tried before L1.
_RCP_L2 = re.compile(r"^(\d{1,2}\.\d{1,2})\.?\s+(.+)$")
_RCP_L1 = re.compile(r"^(\d{1,2})\.\s+(.+)$")
_NOTICE_L1 = re.compile(r"^(\d)\.\s+(.+)$")
_BULLET = re.compile(r"^[-–•]\s+(.+)$")

# A revised/updated date, e.g. "09 septembre 2014" or "07/2020".
_MONTHS = "janvier|février|mars|avril|mai|juin|juillet|août|septembre|octobre|novembre|décembre"
_DATE_RE = re.compile(rf"\d{{1,2}}\s+(?:{_MONTHS})\s+\d{{4}}|\b\d{{2}}/\d{{4}}\b", re.I)

# Vertical gap (points) below which a line continues the current paragraph.
_PARA_GAP = 6.0


def _p(text: str) -> str:
    return f"<p>{html.escape(text)}</p>"


class _NodeBuilder:
    """Accumulate ordered elements into a nested content node-list."""

    def __init__(self, kind: str):
        self.kind = kind  # "rcp" | "notice"
        self.root: list[dict] = []
        self.titre1: dict | None = None
        self.titre2: dict | None = None
        self._para: list[str] = []
        self._para_bold = True
        self._bullets: list[str] = []
        self._prev_y1: float | None = None
        self._prev_page: int | None = None
        self.date_notif = ""
        self._await_date = False  # notice: next line after the "révisée" marker
        # ANNEXE I bundles one SmPC per presentation; keep only the first.
        self._seen_section1 = False
        self._done = False

    # -- sinks -------------------------------------------------------------
    def _sink(self) -> list[dict]:
        if self.titre2 is not None:
            return self.titre2["children"]
        if self.titre1 is not None:
            return self.titre1["children"]
        return self.root

    def _flush_para(self) -> None:
        text = " ".join(self._para).strip()
        if text:
            node_type = "AmmCorpsTexteGras" if self._para_bold else "AmmCorpsTexte"
            self._sink().append({"type": node_type, "content": text, "html": _p(text)})
        self._para = []
        self._para_bold = True

    def _flush_bullets(self) -> None:
        if self._bullets:
            self._sink().append({"type": "listePuce", "content": list(self._bullets)})
        self._bullets = []

    def _flush(self) -> None:
        self._flush_para()
        self._flush_bullets()

    # -- heading handling --------------------------------------------------
    def _open_titre1(self, node_type: str, content: str, number: str) -> None:
        self._flush()
        node = {"type": node_type, "content": content, "anchor": self._anchor(number), "children": []}
        self.root.append(node)
        self.titre1 = node
        self.titre2 = None

    def _open_titre2(self, content: str, number: str) -> None:
        self._flush()
        node = {"type": "AmmAnnexeTitre2", "content": content, "anchor": self._anchor(number), "children": []}
        (self.titre1["children"] if self.titre1 else self.root).append(node)
        self.titre2 = node

    def _anchor(self, number: str) -> str | None:
        table = _RCP_NUMBER_TO_ANCHOR if self.kind == "rcp" else _NOTICE_NUMBER_TO_ANCHOR
        return table.get(number)

    def _try_heading(self, text: str) -> bool:
        if self.kind == "rcp":
            m2 = _RCP_L2.match(text)
            if m2 and m2.group(1) in _RCP_NUMBER_TO_ANCHOR:
                self._open_titre2(text, m2.group(1))
                return True
            m1 = _RCP_L1.match(text)
            if m1 and m1.group(1) in _RCP_NUMBER_TO_ANCHOR:
                if m1.group(1) == "1":
                    if self._seen_section1:
                        self._done = True  # second SmPC starts here — stop.
                        return True
                    self._seen_section1 = True
                self._open_titre1("AmmAnnexeTitre1", text, m1.group(1))
                return True
        else:
            m = _NOTICE_L1.match(text)
            if m and m.group(1) in _NOTICE_NUMBER_TO_ANCHOR:
                self._open_titre1("AmmNoticeTitre1", text, m.group(1))
                return True
        return False

    # -- element dispatch --------------------------------------------------
    def add(self, el) -> None:
        if self._done:
            return
        if isinstance(el, Table):
            self._flush()
            self._sink().append({"type": "table", "tag": "table", "html": el.html, "children": el.children})
            self._prev_y1, self._prev_page = el.y0, el.page
            return

        assert isinstance(el, TextLine)
        text = el.text
        gap = el.y0 - self._prev_y1 if (self._prev_page == el.page and self._prev_y1 is not None) else 1e9
        self._prev_y1, self._prev_page = el.y1, el.page

        if self._await_date:
            self._await_date = False
            m = _DATE_RE.search(text)
            if m:
                self.date_notif = m.group(0)
                return
        if text.startswith("La dernière date") and "révisée" in text:
            self._flush()
            self._await_date = True
            return

        if el.bold and self._try_heading(text):
            return

        mb = _BULLET.match(text)
        if mb:
            self._flush_para()
            self._bullets.append(mb.group(1).strip())
            return

        # Plain body line: continue a bullet, continue a paragraph, or start one.
        if self._bullets and not self._para and gap < _PARA_GAP:
            self._bullets[-1] = f"{self._bullets[-1]} {text}".strip()
        elif self._para and gap < _PARA_GAP:
            self._para.append(text)
            self._para_bold = self._para_bold and el.bold
        else:
            self._flush()
            self._para = [text]
            self._para_bold = el.bold

    def finish(self) -> list[dict]:
        # RCP §10 body may carry the revision date inline.
        if self.kind == "rcp" and not self.date_notif:
            self._scan_section10_date()
        self._flush()
        return self.root

    def _scan_section10_date(self) -> None:
        for node in self.root:
            if node.get("type") == "AmmAnnexeTitre1" and node["content"].startswith("10."):
                for child in node.get("children", []):
                    content = child.get("content")
                    m = _DATE_RE.search(content) if isinstance(content, str) else None
                    if m:
                        self.date_notif = m.group(0)
                        return


def _rcp_title(content: list[dict]) -> str:
    """RCP title = the denomination (first body text under section 1)."""
    for node in content:
        if node.get("type") == "AmmAnnexeTitre1" and node["content"].startswith("1."):
            for child in node.get("children", []):
                if child.get("type") in ("AmmCorpsTexte", "AmmCorpsTexteGras"):
                    return child["content"]
    return ""


def _notice_title(elements: list) -> str:
    """Notice title = the product line right after the 'Notice: …' header."""
    for i, el in enumerate(elements):
        if isinstance(el, TextLine) and el.text.startswith("Notice") and "utilisateur" in el.text:
            for nxt in elements[i + 1 :]:
                if isinstance(nxt, TextLine) and nxt.text.strip():
                    return nxt.text.strip()
    return ""


def _find_ranges(doc: fitz.Document) -> tuple[range | None, range | None]:
    """Locate the ANNEXE I (RCP) and first ANNEXE III.B notice page ranges."""
    annexe: dict[str, int] = {}
    b_notice: int | None = None
    notice_header: int | None = None
    revision: int | None = None

    for pno in range(doc.page_count):
        for block in doc[pno].get_text("dict")["blocks"]:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                spans = line["spans"]
                text = "".join(s["text"] for s in spans).strip()
                bold = bool(spans[0]["flags"] & 16)
                if not bold:
                    continue
                for roman in ("I", "II", "III"):
                    if text == f"ANNEXE {roman}" and roman not in annexe:
                        annexe[roman] = pno
                if text.startswith("B. NOTICE") and b_notice is None:
                    b_notice = pno
                is_notice_header = text.startswith("Notice") and "utilisateur" in text
                if b_notice is not None and notice_header is None and is_notice_header:
                    notice_header = pno
                if notice_header is not None and revision is None and text.startswith("La dernière date"):
                    revision = pno

    rcp_range = None
    if "I" in annexe:
        end = annexe.get("II", doc.page_count)
        rcp_range = range(annexe["I"], end)

    notice_range = None
    if notice_header is not None:
        end = (revision + 1) if revision is not None else doc.page_count
        notice_range = range(notice_header, end)

    return rcp_range, notice_range


def _assemble(body: list[dict], title: str, date_notif: str) -> list[dict]:
    content: list[dict] = []
    if title:
        content.append({"type": "AmmAnnexeTitre", "content": title})
    if date_notif:
        content.append({"type": "DateNotif", "content": date_notif})
    content.extend(body)
    return content


def parse_pdf(pdf_bytes: bytes) -> dict[str, list[dict]]:
    """Parse an EMA PI PDF into ``{"rcp": [...], "notice": [...]}`` node-lists.

    Either list is empty if the corresponding annexe can't be located.
    """
    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    rcp_range, notice_range = _find_ranges(doc)

    result: dict[str, list[dict]] = {"rcp": [], "notice": []}

    if rcp_range is not None:
        elements = extract_elements(doc, rcp_range)
        builder = _NodeBuilder("rcp")
        for el in elements:
            builder.add(el)
        body = builder.finish()
        # Drop the ANNEXE I cover lines ("ANNEXE I", "RÉSUMÉ …") before section 1.
        while body and body[0].get("type") != "AmmAnnexeTitre1":
            body.pop(0)
        result["rcp"] = _assemble(body, _rcp_title(body), builder.date_notif)

    if notice_range is not None:
        elements = extract_elements(doc, notice_range)
        builder = _NodeBuilder("notice")
        for el in elements:
            builder.add(el)
        body = builder.finish()
        result["notice"] = _assemble(body, _notice_title(elements), builder.date_notif)

    return result
