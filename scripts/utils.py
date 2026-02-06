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
