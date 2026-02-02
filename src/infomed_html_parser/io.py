"""File I/O operations for HTML documents."""

import chardet


def charger_html(fichier_html: str) -> str:
    """Load an HTML file with automatic encoding detection."""
    with open(fichier_html, "rb") as f:
        contenu_binaire = f.read()
        encodage = chardet.detect(contenu_binaire)["encoding"]
    return contenu_binaire.decode(encodage)


def charger_liste_cis(fichier_cis: str) -> set[str]:
    """Load the list of CIS codes from a text file."""
    cis_autorises = set()
    with open(fichier_cis, "r", encoding="utf-8") as f:
        for ligne in f:
            cis = ligne.strip()
            if cis:  # Ignore empty lines
                cis_autorises.add(cis)
    return cis_autorises
