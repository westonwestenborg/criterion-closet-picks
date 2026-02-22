"""
Shared utilities for the Criterion Closet Picks data pipeline.
File paths, JSON I/O, slugification, fuzzy matching, env loading.
"""

import json
import os
import re
import time
import unicodedata
from pathlib import Path
from functools import wraps

from dotenv import load_dotenv
from thefuzz import fuzz


# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
CATALOG_FILE = DATA_DIR / "criterion_catalog.json"
GUESTS_FILE = DATA_DIR / "guests.json"
PICKS_RAW_FILE = DATA_DIR / "picks_raw.json"
PICKS_FILE = DATA_DIR / "picks.json"
TRANSCRIPTS_DIR = DATA_DIR / "transcripts"
VALIDATION_DIR = DATA_DIR / "validation"
CHECKPOINT_FILE = DATA_DIR / ".extraction_progress.json"

# Ensure directories exist
DATA_DIR.mkdir(parents=True, exist_ok=True)
TRANSCRIPTS_DIR.mkdir(parents=True, exist_ok=True)
VALIDATION_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------------
# JSON I/O
# ---------------------------------------------------------------------------

def load_json(path: Path) -> list | dict:
    """Load JSON from a file. Returns empty list if file doesn't exist."""
    path = Path(path)
    if not path.exists():
        return []
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def save_json(path: Path, data: list | dict, indent: int = 2) -> None:
    """Save data as JSON, creating parent directories if needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=indent, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Text Processing
# ---------------------------------------------------------------------------

def slugify(text: str) -> str:
    """Convert text to a URL-safe slug."""
    # Normalize unicode characters
    text = unicodedata.normalize("NFKD", text)
    text = text.encode("ascii", "ignore").decode("ascii")
    # Lowercase and replace non-alphanumeric with hyphens
    text = re.sub(r"[^\w\s-]", "", text.lower())
    text = re.sub(r"[-\s]+", "-", text).strip("-")
    return text


def make_film_id(title: str, year: int | None) -> str:
    """Create a unique film ID from title and year."""
    slug = slugify(title)
    if year:
        return f"{slug}-{year}"
    return slug


# ---------------------------------------------------------------------------
# Fuzzy Matching
# ---------------------------------------------------------------------------

def fuzzy_match_name(name1: str, name2: str, threshold: int = 80) -> bool:
    """Check if two names match using fuzzy matching (token sort ratio)."""
    if not name1 or not name2:
        return False
    score = fuzz.token_sort_ratio(name1.lower().strip(), name2.lower().strip())
    return score >= threshold


def fuzzy_match_title(
    title1: str,
    title2: str,
    year1: int | None = None,
    year2: int | None = None,
    threshold: int = 85,
) -> bool:
    """Check if two film titles match, optionally considering year."""
    if not title1 or not title2:
        return False
    score = fuzz.token_sort_ratio(title1.lower().strip(), title2.lower().strip())
    if score >= threshold:
        # If both years are known, they must match (or be within 1 year)
        if year1 and year2:
            return abs(year1 - year2) <= 1
        return True
    return False


def fuzzy_match_score(text1: str, text2: str) -> int:
    """Return the token sort ratio score between two strings."""
    if not text1 or not text2:
        return 0
    return fuzz.token_sort_ratio(text1.lower().strip(), text2.lower().strip())


# ---------------------------------------------------------------------------
# Environment
# ---------------------------------------------------------------------------

def load_env() -> None:
    """Load environment variables from .env file."""
    env_path = PROJECT_ROOT / ".env"
    load_dotenv(env_path)


def get_env(key: str, required: bool = True) -> str | None:
    """Get an environment variable, optionally raising if missing."""
    load_env()
    value = os.environ.get(key)
    if required and not value:
        raise ValueError(f"Missing required environment variable: {key}")
    return value


# ---------------------------------------------------------------------------
# Rate Limiting Helper
# ---------------------------------------------------------------------------

def rate_limit(min_interval: float = 1.0):
    """Decorator that ensures at least min_interval seconds between calls."""
    last_call = [0.0]

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            elapsed = time.time() - last_call[0]
            if elapsed < min_interval:
                time.sleep(min_interval - elapsed)
            result = func(*args, **kwargs)
            last_call[0] = time.time()
            return result
        return wrapper
    return decorator


# ---------------------------------------------------------------------------
# Logging helpers
# ---------------------------------------------------------------------------

def log(msg: str) -> None:
    """Print a timestamped log message."""
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}")


# ---------------------------------------------------------------------------
# Pilot guest list
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Multi-visit guest URLs
# ---------------------------------------------------------------------------

# Criterion page URLs per visit (visit 1 = older/lower collection ID, visit 2 = newer)
# Scraped from criterion.com/shop#collection-closet-picks index
VISIT_CRITERION_URLS = {
    "bill-hader": [
        "https://www.criterion.com/shop/collection/488-bill-hader-s-closet-picks-2011",
        "https://www.criterion.com/shop/collection/720-bill-hader-s-closet-picks",
    ],
    "guillermo-del-toro": [
        "https://www.criterion.com/shop/collection/645-guillermo-del-toro-s-closet-picks",
        "https://www.criterion.com/shop/collection/911-guillermo-del-toro-s-closet-picks",
    ],
    "ari-aster": [
        "https://www.criterion.com/shop/collection/544-ari-aster-s-closet-picks-2023",
        "https://www.criterion.com/shop/collection/856-ari-aster-s-closet-picks",
    ],
    "michael-cera": [
        "https://www.criterion.com/shop/collection/491-michael-cera-s-closet-picks-2014",
        "https://www.criterion.com/shop/collection/823-michael-cera-s-closet-picks",
    ],
    "yorgos-lanthimos": [
        "https://www.criterion.com/shop/collection/467-yorgos-lanthimos-ariane-labed-s-closet-picks",
        "https://www.criterion.com/shop/collection/900-yorgos-lanthimos-s-closet-picks",
    ],
    "edgar-wright": [
        "https://www.criterion.com/shop/collection/490-edgar-wright-s-closet-picks",
        "https://www.criterion.com/shop/collection/887-edgar-wright-s-closet-picks",
    ],
    "benny-safdie": [
        "https://www.criterion.com/shop/collection/476-josh-and-benny-safdie-s-closet-picks",
        "https://www.criterion.com/shop/collection/880-benny-safdie-s-closet-picks",
    ],
    # Single-page guests: assign to visit 1 only
    "barry-jenkins": [
        "https://www.criterion.com/shop/collection/470-barry-jenkins-s-closet-picks",
    ],
    "isabelle-huppert": [
        "https://www.criterion.com/shop/collection/741-isabelle-huppert-s-closet-picks",
    ],
    "griffin-dunne": [
        "https://www.criterion.com/shop/collection/795-griffin-dunne-s-closet-picks",
    ],
    "wim-wenders": [
        "https://www.criterion.com/shop/collection/634-wim-wenders-closet-picks",
    ],
    # Guests whose collection pages aren't on the index but exist on criterion.com
    "jason-bateman": [
        "https://www.criterion.com/shop/collection/881-jason-bateman-s-closet-picks",
    ],
    "ben-whishaw": [
        "https://www.criterion.com/shop/collection/885-ben-whishaw-s-closet-picks",
    ],
    "franklin-leonard": [
        "https://www.criterion.com/shop/collection/802-franklin-leonard-s-closet-picks",
    ],
    "hans-zimmer": [
        "https://www.criterion.com/shop/collection/793-hans-zimmer-s-closet-picks",
    ],
    "daniels": [
        "https://www.criterion.com/shop/collection/522-daniels-closet-picks",
    ],
    "five-comics": [
        "https://www.criterion.com/shop/collection/443-five-comics-closet-picks",
    ],
}


# ---------------------------------------------------------------------------
# Excluded YouTube video IDs
# ---------------------------------------------------------------------------

# Videos in the Closet Picks playlist that are NOT individual guest episodes.
# These are filtered out when checking for new videos to avoid false positives.
EXCLUDED_VIDEO_IDS = {
    # Compilations / "We Love" series
    "2SS0RQzGvds",  # From Criterion, With Love | Closet Picks Edition
    "ujVHv-tdoxk",  # We Love David Lynch | Closet Picks Edition
    "ATn7YObIedU",  # We Love Jim Jarmusch | Closet Picks Edition
    "u1ou-tqLgJo",  # We Love ALL THAT JAZZ | Closet Picks Edition
    "X2x1-t3GDZA",  # We Love Akira Kurosawa | Closet Picks Edition
    "LG3qEVw3bfA",  # We Love NIGHT OF THE LIVING DEAD | Closet Picks Edition
    "TnQsap0KJvY",  # We Love Richard Linklater | Closet Picks Edition
    "GAbJr5c5OLU",  # We Love Martin Scorsese | Closet Picks Edition
    "zE9yZdXh0s8",  # Closet Picks: Greatest Hits
    # Mobile Closet events
    "ujTba4LsZVs",  # The Criterion Closet: Chicago Edition
    "8ZWOaTAiKL8",  # Criterion Mobile Closet TIFF promo
    "qE7j_1UOPXI",  # Mobile Closet LA Aero Theatre
    "LnrhF636NJ8",  # Mobile Closet Vidiots LA
    "YOXcZIRFEsw",  # Mobile Closet Returns to LA
    "iXHk5FpPhDc",  # Mobile Closet SXSW
    "-KYLPII0964",  # Mobile Closet Brooklyn
    "XvX8AGKHKCE",  # Mobile Closet NYFF Weekend 2
    "jdceKguPiAI",  # Mobile Closet NYFF Weekend 1
    # Special messages / promos
    "Vp0T97ClTXs",  # Special Message from Richard Linklater
    "X48hhzz-5eM",  # Mother's Day Weekend moment
    "Rf530YO8J-k",  # Cillian Murphy + THE WES ANDERSON ARCHIVE (book promo)
    # Single-film spotlight clips (not full episodes)
    "kg6B7HL5jAI",  # Lucy Liu on IN THE MOOD FOR LOVE
    "tiafmmKAUBE",  # Wagner Moura on LIMITE
    "vFp3cXxveGc",  # Ryan Coogler on MALCOLM X
    # DVD-era picks (different format, pre-Closet Picks series)
    "f31TXhdC-Ps",  # Mike Leigh's DVD Picks
    "PtVA6nKygFs",  # Wim Wenders' DVD Picks
    # Private/unavailable videos
    "e82armU7LeI",  # [Private video]
    "bDGl-r3dGtM",  # [Private video]
}


PILOT_GUESTS = [
    "Barry Jenkins",
    "Guillermo del Toro",
    "Bill Hader",
    "Denis Villeneuve",
    "Bong Joon-ho",
    "Ayo Edebiri",
    "Charli XCX",
    "Andrew Garfield",
    "Park Chan-wook",
    "Cate Blanchett",
]

# Note: Greta Gerwig and Martin Scorsese were in the original pilot list but
# do not have Letterboxd @closetpicks lists. Replaced with Denis Villeneuve
# and Andrew Garfield. Cate Blanchett's list is joint with Todd Field.
