"""Match a CIS to its presentation within a multi-presentation EMA PDF.

One EMA PDF bundles one SmPC/Notice per device (cartouche, KwikPen, Tempo Pen…).
Several CIS share the PDF, one per presentation. We pick, for each CIS, the
presentation whose denomination best matches the CIS's ``SpecDenom01`` using
Jaccard similarity over normalized tokens. Jaccard naturally prefers the tighter
match — e.g. a generic "…stylo prérempli" CIS maps to the KwikPen presentation
rather than "Tempo Pen …stylo prérempli", whose extra tokens lower the overlap.
"""

import re
import unicodedata


def _tokens(text: str) -> set[str]:
    folded = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")
    return set(re.findall(r"[a-z0-9]+", folded.lower()))


def match_presentation(spec_denom: str, docs: list[dict]) -> dict | None:
    """Return the presentation doc best matching ``spec_denom``, or None.

    ``docs`` are the ``{"denomination": …, …}`` dicts from ``parse_pdf``. A single
    presentation is returned unconditionally (unambiguous). With several
    presentations and no usable name (empty ``spec_denom``), returns None so the
    caller skips the CIS rather than publishing a possibly-wrong presentation.
    Ties keep the earliest doc.
    """
    if not docs:
        return None
    if len(docs) == 1:
        return docs[0]

    query = _tokens(spec_denom)
    if not query:
        return None  # no signal to disambiguate multiple presentations

    best, best_score = None, 0.0
    for doc in docs:
        candidate = _tokens(doc.get("denomination", ""))
        union = len(query | candidate)
        score = len(query & candidate) / union if union else 0.0
        if score > best_score:
            best, best_score = doc, score
    return best
