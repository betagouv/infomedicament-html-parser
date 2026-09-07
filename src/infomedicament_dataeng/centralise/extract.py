"""Extract ordered text lines and tables from an EMA QRD-template PDF.

PyMuPDF gives us per-page *visual* lines (spans with font metadata) and native
tables. This module turns a page range into a flat, reading-order stream of
``TextLine`` / ``Table`` elements that the semantic parser consumes.

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
class TextRun:
    """A contiguous piece of text sharing the same inline formatting."""

    text: str
    bold: bool = False
    underline: bool = False


@dataclass
class TextLine:
    text: str
    size: float
    bold: bool
    page: int
    y0: float
    y1: float
    runs: list[TextRun] | None = None


@dataclass
class Table:
    html: str
    page: int
    y0: float


@dataclass
class Image:
    data: bytes  # raw embedded image bytes (uploaded to the CDN by the caller)
    ext: str  # "png" / "jpeg" / …
    page: int
    y0: float


# Ignore tiny embedded bitmaps (icons, separators, soft masks); real QRD figures
# are far larger (the smallest injection-diagram in our fixtures is ~108px).
_MIN_IMAGE_PX = 32


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


def _merge_same_line(lines: list[TextLine]) -> list[TextLine]:
    """Merge consecutive lines sharing a baseline (number+title, bullet+text)."""
    merged: list[TextLine] = []
    for ln in lines:
        if merged and ln.page == merged[-1].page and abs(ln.y0 - merged[-1].y0) < _SAME_LINE_TOL:
            prev = merged[-1]
            combined_runs = (
                _merge_runs([*_line_runs(prev), TextRun(" "), *_line_runs(ln)])
                if prev.runs is not None or ln.runs is not None
                else None
            )
            # The longer fragment decides boldness (the title, not the "1." / "-").
            bold = ln.bold if len(ln.text.strip()) > len(prev.text.strip()) else prev.bold
            prev.text = f"{prev.text} {ln.text}".strip()
            prev.bold = bold
            prev.y1 = max(prev.y1, ln.y1)
            prev.runs = combined_runs
        else:
            merged.append(ln)
    return merged


def _line_runs(line: TextLine) -> list[TextRun]:
    if line.runs is not None:
        return line.runs
    return [TextRun(line.text, bold=line.bold)]


def _merge_runs(runs: list[TextRun]) -> list[TextRun]:
    merged: list[TextRun] = []
    for run in runs:
        if not run.text:
            continue
        if merged and (merged[-1].bold, merged[-1].underline) == (run.bold, run.underline):
            merged[-1].text += run.text
        else:
            merged.append(TextRun(run.text, run.bold, run.underline))
    return merged


def _underline_rects(page: fitz.Page) -> list[fitz.Rect]:
    """Return thin horizontal drawings that can represent PDF underlines."""
    return [
        fitz.Rect(drawing["rect"])
        for drawing in page.get_drawings()
        if drawing["rect"].width > 1 and drawing["rect"].height <= 1.5
    ]


def _char_is_underlined(char_bbox, underline_rects: list[fitz.Rect]) -> bool:
    char = fitz.Rect(char_bbox)
    if char.width <= 0:
        return False
    for underline in underline_rects:
        # Word-generated PDFs place the rule around 1.2pt above the bottom of
        # PyMuPDF's character box. Requiring horizontal character overlap keeps
        # nearby table borders and separators from becoming false positives.
        if not -2.5 <= underline.y0 - char.y1 <= 1.5:
            continue
        overlap = max(0.0, min(char.x1, underline.x1) - max(char.x0, underline.x0))
        if overlap >= char.width * 0.45:
            return True
    return False


def _extract_runs(spans: list[dict], underline_rects: list[fitz.Rect]) -> list[TextRun]:
    runs: list[TextRun] = []
    for span in spans:
        bold = bool(span["flags"] & _BOLD_FLAG)
        for char in span["chars"]:
            value = char["c"]
            underline = bool(value.strip()) and _char_is_underlined(char["bbox"], underline_rects)
            if runs and (runs[-1].bold, runs[-1].underline) == (bold, underline):
                runs[-1].text += value
            else:
                runs.append(TextRun(value, bold, underline))

    while runs:
        runs[0].text = runs[0].text.lstrip()
        if runs[0].text:
            break
        runs.pop(0)
    while runs:
        runs[-1].text = runs[-1].text.rstrip()
        if runs[-1].text:
            break
        runs.pop()
    return _merge_runs(runs)


def extract_elements(doc: fitz.Document, page_range: range) -> list:
    """Return an ordered ``TextLine``/``Table`` stream for the given pages."""
    elements: list = []
    for pno in page_range:
        page = doc[pno]
        tables = page.find_tables().tables
        table_rects = [fitz.Rect(t.bbox) for t in tables]
        underline_rects = _underline_rects(page)

        page_lines: list[TextLine] = []
        for block in page.get_text("rawdict")["blocks"]:
            if "lines" not in block:
                continue
            for line in block["lines"]:
                spans = line["spans"]
                runs = _extract_runs(spans, underline_rects)
                text = "".join(run.text for run in runs)
                if not text:
                    continue
                span = max(spans, key=lambda s: len(s["chars"]))
                if span["size"] < _MIN_BODY_SIZE:
                    continue
                x0, y0, x1, y1 = line["bbox"]
                center = fitz.Point((x0 + x1) / 2, (y0 + y1) / 2)
                if any(center in r for r in table_rects):
                    continue
                page_lines.append(
                    TextLine(
                        text=text,
                        size=span["size"],
                        bold=bool(span["flags"] & _BOLD_FLAG),
                        page=pno,
                        y0=y0,
                        y1=y1,
                        runs=runs,
                    )
                )

        page_elems: list = _merge_same_line(page_lines)
        for t, rect in zip(tables, table_rects):
            rows = t.extract()
            page_elems.append(Table(html=_table_to_html(rows), page=pno, y0=rect.y0))

        page_elems.extend(_page_images(doc, page, pno))

        page_elems.sort(key=lambda e: e.y0)
        elements.extend(page_elems)
    return elements


def _page_images(doc: fitz.Document, page: fitz.Page, pno: int) -> list:
    """Extract embedded images on a page as positioned ``Image`` elements."""
    out: list = []
    for img in page.get_images(full=True):
        xref = img[0]
        info = doc.extract_image(xref)
        data = info.get("image")
        if not data or info.get("width", 0) < _MIN_IMAGE_PX or info.get("height", 0) < _MIN_IMAGE_PX:
            continue
        for rect in page.get_image_rects(xref):
            out.append(Image(data=data, ext=info.get("ext", "png"), page=pno, y0=rect.y0))
    return out
