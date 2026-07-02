"""Extract ordered text lines and tables from an EMA QRD-template PDF.

PyMuPDF gives us per-page *visual* lines (spans with font metadata) and native
tables. This module turns a page range into a flat, reading-order stream of
``TextLine`` / ``Table`` elements that the segmenter (``parser.py``) consumes.

Two PDF quirks are handled here so the segmenter can stay simple:
- QRD headings split the number and the title into two same-baseline "lines"
  (e.g. ``"1."`` then ``"DÉNOMINATION…"``); we merge lines at the same y.
- Table text is also returned as ordinary lines, so we drop any line inside a
  table's bounding box and emit a single ``Table`` element in its place.
"""

import html
from dataclasses import dataclass

import fitz

# Font size of the running page-number footer/header noise; anything smaller
# than a body line (11pt) is discarded.
_MIN_BODY_SIZE = 9.0
# Two "lines" whose top y differ by less than this are the same visual line.
_SAME_LINE_TOL = 3.0
_BOLD_FLAG = 16


@dataclass
class TextLine:
    text: str
    size: float
    bold: bool
    page: int
    y0: float
    y1: float


@dataclass
class Table:
    html: str
    children: list  # row/cell tree; not stored, but required for the importer guard
    page: int
    y0: float


def _clean_cell(cell) -> str:
    return " ".join((cell or "").split())


def _table_to_html(rows: list[list]) -> str:
    """Render extracted table rows (list of lists of cell strings) as HTML."""
    out = ["<table><tbody>"]
    for row in rows:
        out.append("<tr>")
        for cell in row:
            out.append(f"<td>{html.escape(_clean_cell(cell))}</td>")
        out.append("</tr>")
    out.append("</tbody></table>")
    return "".join(out)


def _table_children(rows: list[list]) -> list:
    """Row/cell tree mirroring html_vers_json; kept only so the node passes the
    importer's truthiness guard (table children are ignored on insert)."""
    return [
        {"tag": "tr", "children": [{"tag": "td", "text": _clean_cell(c), "children": []} for c in row]} for row in rows
    ]


def _merge_same_line(lines: list[TextLine]) -> list[TextLine]:
    """Merge consecutive lines sharing a baseline (number+title, bullet+text)."""
    merged: list[TextLine] = []
    for ln in lines:
        if merged and ln.page == merged[-1].page and abs(ln.y0 - merged[-1].y0) < _SAME_LINE_TOL:
            prev = merged[-1]
            # The longer fragment decides boldness (the title, not the "1." / "-").
            bold = ln.bold if len(ln.text.strip()) > len(prev.text.strip()) else prev.bold
            prev.text = f"{prev.text} {ln.text}".strip()
            prev.bold = bold
            prev.y1 = max(prev.y1, ln.y1)
        else:
            merged.append(ln)
    return merged


def extract_elements(doc: fitz.Document, page_range: range) -> list:
    """Return an ordered ``TextLine``/``Table`` stream for the given pages."""
    elements: list = []
    for pno in page_range:
        page = doc[pno]
        tables = page.find_tables().tables
        table_rects = [fitz.Rect(t.bbox) for t in tables]

        page_lines: list[TextLine] = []
        for block in page.get_text("dict")["blocks"]:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                spans = line["spans"]
                text = "".join(s["text"] for s in spans).strip()
                if not text:
                    continue
                span = max(spans, key=lambda s: len(s["text"]))
                if span["size"] < _MIN_BODY_SIZE:
                    continue
                x0, y0, x1, y1 = line["bbox"]
                center = fitz.Point((x0 + x1) / 2, (y0 + y1) / 2)
                if any(center in r for r in table_rects):
                    continue
                page_lines.append(
                    TextLine(
                        text=text, size=span["size"], bold=bool(span["flags"] & _BOLD_FLAG), page=pno, y0=y0, y1=y1
                    )
                )

        page_elems: list = _merge_same_line(page_lines)
        for t, rect in zip(tables, table_rects):
            rows = t.extract()
            page_elems.append(Table(html=_table_to_html(rows), children=_table_children(rows), page=pno, y0=rect.y0))

        page_elems.sort(key=lambda e: e.y0)
        elements.extend(page_elems)
    return elements
