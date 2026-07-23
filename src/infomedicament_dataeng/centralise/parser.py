"""Segment an EMA QRD-template PDF into RCP + Notice content node-lists.

Emits the same node shapes the importer consumes (see the plan / ``db.py``):
``AmmAnnexeTitre``/``DateNotif`` metadata, ``AmmAnnexeTitre1/2`` &
``AmmNoticeTitre1`` headings, ``AmmCorpsTexte``/``AmmCorpsTexteGras`` bodies,
``listePuce`` bullets, and ``table`` nodes. Anchors are looked up from the
authoritative maps in ``opensearch.sections`` (no duplication).
"""

import hashlib
import html
import re

import fitz

from ..config import get_config
from ..opensearch.sections import _NOTICE_NUMBER_TO_ANCHOR, _RCP_NUMBER_TO_ANCHOR
from .extract import Image, Table, TextLine, extract_elements

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

# Cap on the product-name lines collected under a notice header (before the INN).
_MAX_DENOM_LINES = 12


def _p(text: str) -> str:
    return f"<p>{html.escape(text)}</p>"


def _is_notice_header(text: str) -> bool:
    """Match the QRD notice header across wordings:
    'Notice: Information de l'utilisateur' / 'Notice : Information du patient'."""
    return text.startswith("Notice") and "information" in text.lower()


class _NodeBuilder:
    """Accumulate ordered elements into a nested content node-list."""

    def __init__(self, kind: str, cdn_base_url: str, image_prefix: str, images: dict[str, bytes]):
        self.kind = kind  # "rcp" | "notice"
        self.cdn_base_url = cdn_base_url  # e.g. ".../exports/images"
        self.image_prefix = image_prefix  # S3 key prefix, e.g. "exports/images/"
        self.images = images  # shared sha-keyed blob map, uploaded to the CDN by the caller
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
        if isinstance(el, Table):
            self._flush()
            self._sink().append(
                {"type": "AmmCorpsTexteTable", "tag": "table", "html": el.html, "children": el.children}
            )
            self._prev_y1, self._prev_page = el.y0, el.page
            return

        if isinstance(el, Image):
            self._flush()
            self._sink().append(self._image_node(el))
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

    def _image_node(self, el: Image) -> dict:
        """Register the blob for upload and return an ANSM-style image node.

        Mirrors the ANSM shape: the ``<img>`` lives in the ``html`` field (which the
        frontend renders). That renderer is only reached when ``content`` is also
        truthy, and the importer drops content-less nodes — hence the space in
        ``content`` (unused: the html branch renders first).
        """
        sha = hashlib.sha256(el.data).hexdigest()
        ext = el.ext or "png"
        self.images[f"{self.image_prefix}centralise/{sha}.{ext}"] = el.data
        url = f"{self.cdn_base_url}/centralise/{sha}.{ext}"
        return {
            "type": "AmmCorpsTexte",
            "content": " ",
            "html": f'<p><img src="{url}" alt="" style="max-width:100%"/></p>',
        }


def _rcp_denomination(body: list[dict]) -> str:
    """RCP denomination = the body text under section 1 (used to match a CIS).

    A single SmPC can cover several devices (e.g. KwikPen + Tempo Pen), so all
    of section 1's body lines are joined, not just the first.
    """
    for node in body:
        if node.get("type") == "AmmAnnexeTitre1" and node["content"].startswith("1."):
            parts = [c["content"] for c in node.get("children", []) if isinstance(c.get("content"), str)]
            return " ".join(parts)
    return ""


def _notice_denomination(elements: list) -> str:
    """Notice denomination = the product lines after the 'Notice: …' header.

    One notice can cover several strengths, each on its own line (Exelon 1,5 / 3,0 /
    4,5 / 6,0 mg), so all of them are joined — as ``_rcp_denomination`` does for
    section 1. The block ends at the INN/boilerplate that follows the names.
    """
    for i, el in enumerate(elements):
        if not (isinstance(el, TextLine) and _is_notice_header(el.text)):
            continue
        names: list[str] = []
        for nxt in elements[i + 1 :]:
            if not isinstance(nxt, TextLine):
                continue
            text = nxt.text.strip()
            if not text:
                continue
            if text.lower().startswith("veuillez lire") or _NOTICE_L1.match(text):
                break
            names.append(text)
            if len(names) >= _MAX_DENOM_LINES:
                break
        return " ".join(names)
    return ""


def _find_annexe1_range(doc: fitz.Document) -> range | None:
    """Page range of ANNEXE I (the RCP annexe), up to ANNEXE II."""
    start = end = None
    for pno in range(doc.page_count):
        for text, bold in _bold_lines(doc[pno]):
            if bold and text == "ANNEXE I" and start is None:
                start = pno
            elif bold and text == "ANNEXE II" and end is None:
                end = pno
    if start is None:
        return None
    return range(start, end if end is not None else doc.page_count)


def _find_notice_ranges(doc: fitz.Document) -> list[range]:
    """Page range of each ANNEXE III.B notice (one per presentation).

    A notice runs from its 'Notice: … information' header to just before the next
    such header (or the end of the document for the last one). This deliberately
    includes the illustrated 'Manuel d'utilisation' (instructions-for-use) that
    follows the notice text — its injection diagrams are part of the leaflet.
    The revision date inside the notice is still captured by the node builder.
    """
    b_notice = None
    headers: list[int] = []
    for pno in range(doc.page_count):
        for text, bold in _bold_lines(doc[pno]):
            if not bold:
                continue
            if text.startswith("B. NOTICE") and b_notice is None:
                b_notice = pno
            if b_notice is not None and _is_notice_header(text):
                headers.append(pno)

    ranges: list[range] = []
    for i, header in enumerate(headers):
        nxt = headers[i + 1] if i + 1 < len(headers) else doc.page_count
        ranges.append(range(header, nxt))
    return ranges


def _bold_lines(page: fitz.Page):
    """Yield (text, is_bold) for each non-empty line on the page."""
    for block in page.get_text("dict")["blocks"]:
        if "lines" not in block:
            continue
        for line in block["lines"]:
            spans = line["spans"]
            text = "".join(s["text"] for s in spans).strip()
            if text:
                yield text, bool(spans[0]["flags"] & 16)


def _split_smpcs(elements: list) -> list[list]:
    """Split an ANNEXE I element stream into one group per SmPC (at each §1)."""
    groups: list[list] = []
    current: list | None = None
    for el in elements:
        if isinstance(el, TextLine) and el.bold:
            m = _RCP_L1.match(el.text)
            if m and m.group(1) == "1":  # a new "1. DÉNOMINATION" starts a SmPC
                current = []
                groups.append(current)
        if current is not None:
            current.append(el)
    return groups


def _build_body(
    elements: list, kind: str, images: dict, cdn_base_url: str, image_prefix: str
) -> tuple[list[dict], str]:
    """Run the node builder over one presentation's elements; return (body, date)."""
    builder = _NodeBuilder(kind, cdn_base_url, image_prefix, images)
    for el in elements:
        builder.add(el)
    return builder.finish(), builder.date_notif


def assemble_document(body: list[dict], title: str, date_notif: str) -> list[dict]:
    """Prepend the AmmAnnexeTitre / DateNotif metadata nodes to a body."""
    content: list[dict] = []
    if title:
        content.append({"type": "AmmAnnexeTitre", "content": title})
    if date_notif:
        content.append({"type": "DateNotif", "content": date_notif})
    content.extend(body)
    return content


def parse_pdf(pdf_bytes: bytes) -> dict:
    """Parse an EMA PI PDF into all its RCP and Notice presentations.

    Returns ``{"rcp": [doc, …], "notice": [doc, …], "images": {s3_key: bytes}}``
    where each ``doc`` is ``{"denomination": str, "date_notif": str, "content":
    [body nodes]}``. One PDF bundles one presentation per device (cartouche, pen,
    …); the caller matches each CIS to its presentation (see ``match.py``),
    prepends the metadata via ``assemble_document``, and uploads ``images`` to
    the CDN (they are content-addressed, so uploads are idempotent).
    """
    cfg = get_config()
    cdn_base_url, image_prefix = cfg.cdn_base_url, cfg.s3.image_prefix

    doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    images: dict[str, bytes] = {}
    result: dict = {"rcp": [], "notice": [], "images": images}

    annexe1 = _find_annexe1_range(doc)
    if annexe1 is not None:
        for group in _split_smpcs(extract_elements(doc, annexe1)):
            body, date_notif = _build_body(group, "rcp", images, cdn_base_url, image_prefix)
            if body:
                result["rcp"].append(
                    {"denomination": _rcp_denomination(body), "date_notif": date_notif, "content": body}
                )

    for rng in _find_notice_ranges(doc):
        elements = extract_elements(doc, rng)
        body, date_notif = _build_body(elements, "notice", images, cdn_base_url, image_prefix)
        if body:
            result["notice"].append(
                {"denomination": _notice_denomination(elements), "date_notif": date_notif, "content": body}
            )

    return result
