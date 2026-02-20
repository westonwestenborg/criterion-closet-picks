#!/usr/bin/env python3
"""
Diagnostic script: Explore Criterion.com box set pages to discover
what structured data is available for enrichment.

Fetches a sample of box set pages and reports all extractable fields.
Not a permanent pipeline step -- just for exploration.

Usage:
  python scripts/explore_box_set_pages.py
"""

import re
import sys
import time

import cloudscraper
from bs4 import BeautifulSoup

sys.path.insert(0, str(__import__("pathlib").Path(__file__).resolve().parent.parent))
from scripts.utils import log

RATE_LIMIT_SECONDS = 1.5

# Sample box sets: variety of sizes and types
# URLs sourced from criterion_catalog.json (some differ from guessed slugs)
SAMPLE_URLS = [
    ("John Cassavetes: Five Films", "https://www.criterion.com/boxsets/558-john-cassavetes-five-films"),
    ("Godzilla: The Showa-Era Films", "https://www.criterion.com/boxsets/2648-godzilla-the-showa-era-films-1954-1975"),
    ("The BRD Trilogy", "https://www.criterion.com/boxsets/138-the-brd-trilogy"),
    ("World of Wong Kar Wai", "https://www.criterion.com/boxsets/4117-world-of-wong-kar-wai"),
    ("Ingmar Bergman's Cinema", "https://www.criterion.com/boxsets/1427-ingmar-bergman-s-cinema"),
]


def extract_data(soup: BeautifulSoup, url: str) -> dict:
    """Extract all discoverable structured data from a box set page."""
    data = {}

    # --- Title ---
    title_el = soup.select_one("h1")
    if title_el:
        data["title"] = title_el.get_text(strip=True)

    # --- Director(s) ---
    # Try various selectors for director info
    for sel in [
        ".film-director", ".director", "h2.subtitle",
        '[data-director]', '.boxset-director',
    ]:
        el = soup.select_one(sel)
        if el:
            data["director"] = el.get_text(strip=True)
            data["director_selector"] = sel
            break

    # Also check for "Directed by" text pattern anywhere in top area
    if "director" not in data:
        for p in soup.select("p, span, div.subtitle, .film-info"):
            text = p.get_text(strip=True)
            m = re.match(r"(?:Directed by|A film by|Films by)\s+(.+)", text, re.I)
            if m:
                data["director"] = m.group(1)
                data["director_selector"] = "text pattern"
                break

    # --- Year / Year Range ---
    # Look for years in title area and metadata
    title_text = soup.select_one("title")
    if title_text:
        data["page_title"] = title_text.get_text(strip=True)
        m = re.search(r"\((\d{4}(?:\s*[-–]\s*\d{4})?)\)", title_text.get_text())
        if m:
            data["year"] = m.group(1)

    # --- Description / Synopsis ---
    for sel in [
        ".product-description", ".boxset-description", ".synopsis",
        ".product-about", ".about-text", "section.description",
        '[data-description]',
    ]:
        el = soup.select_one(sel)
        if el:
            text = el.get_text(strip=True)
            if len(text) > 20:
                data["description"] = text[:300] + ("..." if len(text) > 300 else "")
                data["description_selector"] = sel
                data["description_full_length"] = len(text)
                break

    # Fallback: look for long <p> blocks near top
    if "description" not in data:
        for p in soup.select("p"):
            text = p.get_text(strip=True)
            if len(text) > 100 and not any(skip in text.lower() for skip in ["cookie", "privacy", "subscribe"]):
                data["description"] = text[:300] + ("..." if len(text) > 300 else "")
                data["description_selector"] = "first long <p>"
                data["description_full_length"] = len(text)
                break

    # --- Box Art Image ---
    for sel in [".product-box-art img", ".boxset-hero img", 'meta[property="og:image"]']:
        if sel.startswith("meta"):
            el = soup.select_one(sel)
            if el and el.get("content"):
                data["image_url"] = el["content"]
                data["image_selector"] = sel
                break
        else:
            el = soup.select_one(sel)
            if el and el.get("src"):
                data["image_url"] = el["src"]
                data["image_selector"] = sel
                break

    # --- Film Meta (box set-level metadata) ---
    film_meta = soup.select_one(".film-meta")
    if film_meta:
        data["film_meta_html"] = film_meta.get_text(separator=" | ", strip=True)[:300]
        # Extract individual items from .film-meta-list
        meta_items = film_meta.select(".film-meta-list li, .film-meta-list span, .film-meta-list div")
        if meta_items:
            data["film_meta_items"] = [el.get_text(strip=True) for el in meta_items if el.get_text(strip=True)]

    # --- Included Films (using discovered .film-set structure) ---
    films = []
    film_sets = soup.select(".film-set")
    if film_sets:
        for fs in film_sets:
            film = {}
            title_el = fs.select_one(".film-set-title a")
            if title_el:
                film["title"] = title_el.get_text(strip=True)
                film["url"] = title_el.get("href", "")
            year_el = fs.select_one(".film-set-year")
            if year_el:
                film["year"] = year_el.get_text(strip=True)
            descrip_el = fs.select_one(".film-set-descrip")
            if descrip_el:
                film["description_preview"] = descrip_el.get_text(strip=True)[:120]
            if film.get("title"):
                films.append(film)
        if films:
            data["films_selector"] = ".film-set"

    # Fallback: a[href*="/films/"] links
    if not films:
        for sel in [
            'a[href*="/films/"]',
        ]:
            links = soup.select(sel)
            if links:
                for a in links:
                    href = a.get("href", "")
                    title = a.get_text(strip=True)
                    if "/films/" in href and title and len(title) > 1:
                        films.append({"title": title, "url": href})
                if films:
                    data["films_selector"] = sel
                    break

    # Deduplicate by URL
    seen_urls = set()
    unique_films = []
    for f in films:
        url_key = f.get("url", f.get("title", ""))
        if url_key not in seen_urls:
            seen_urls.add(url_key)
            unique_films.append(f)
    data["included_films"] = unique_films
    data["included_films_count"] = len(unique_films)

    # --- Special Features ---
    for sel in [".special-features", ".features-list", ".product-features"]:
        el = soup.select_one(sel)
        if el:
            items = [li.get_text(strip=True) for li in el.select("li")]
            if items:
                data["special_features"] = items[:10]  # Cap for readability
                data["special_features_selector"] = sel
                data["special_features_count"] = len(items)
                break

    # --- Country ---
    for sel in [".film-country", ".country", '[data-country]']:
        el = soup.select_one(sel)
        if el:
            data["country"] = el.get_text(strip=True)
            data["country_selector"] = sel
            break

    # --- Meta tags ---
    meta_data = {}
    for meta in soup.select("meta"):
        prop = meta.get("property", meta.get("name", ""))
        content = meta.get("content", "")
        if prop and content and prop.startswith("og:"):
            meta_data[prop] = content[:200]
    if meta_data:
        data["og_meta"] = meta_data

    # --- Discover all class names in main content area ---
    main = soup.select_one("main, #main, .main-content, article, .product-page")
    if main:
        classes = set()
        for el in main.find_all(True):
            for cls in el.get("class", []):
                classes.add(cls)
        data["content_area_classes"] = sorted(classes)
        data["content_area_tag"] = main.name + (f".{'.'.join(main.get('class', []))}" if main.get("class") else "")

    # --- Discover all section/heading structure ---
    headings = []
    for h in soup.select("h1, h2, h3, h4"):
        parent_classes = ".".join(h.parent.get("class", [])) if h.parent else ""
        headings.append({
            "tag": h.name,
            "text": h.get_text(strip=True)[:80],
            "parent_class": parent_classes,
        })
    data["heading_structure"] = headings

    return data


def print_report(label: str, url: str, data: dict) -> None:
    """Print a readable report for one box set."""
    print(f"\n{'=' * 70}")
    print(f"  {label}")
    print(f"  {url}")
    print(f"{'=' * 70}")

    # Core fields
    fields = [
        ("Title", data.get("title")),
        ("Page Title", data.get("page_title")),
        ("Year", data.get("year")),
        ("Director", data.get("director")),
        ("Director Selector", data.get("director_selector")),
        ("Country", data.get("country")),
        ("Image URL", data.get("image_url", "")[:80] + "..." if data.get("image_url") else None),
        ("Image Selector", data.get("image_selector")),
    ]
    print("\n  --- Core Fields ---")
    for label_str, value in fields:
        status = "FOUND" if value else "MISSING"
        print(f"  [{status:7s}] {label_str}: {value or '---'}")

    # Film meta (box set-level metadata block)
    if data.get("film_meta_html"):
        print(f"\n  --- Film Meta Block (.film-meta) ---")
        print(f"  Raw: {data['film_meta_html'][:200]}")
        if data.get("film_meta_items"):
            for item in data["film_meta_items"]:
                print(f"    - {item}")

    # Description
    print("\n  --- Description ---")
    if data.get("description"):
        print(f"  [FOUND  ] via {data.get('description_selector')} ({data.get('description_full_length')} chars)")
        print(f"  {data['description'][:200]}")
    else:
        print("  [MISSING] No description found")

    # Included films
    print(f"\n  --- Included Films ({data.get('included_films_count', 0)}) ---")
    if data.get("included_films"):
        print(f"  Selector: {data.get('films_selector')}")
        for f in data["included_films"][:10]:
            year = f.get("year", "")
            year_str = f" ({year})" if year else ""
            desc = f.get("description_preview", "")
            desc_str = f"  -- {desc}" if desc else ""
            print(f"    - {f['title']}{year_str}  [{f.get('url', '')}]{desc_str}")
        if data["included_films_count"] > 10:
            print(f"    ... and {data['included_films_count'] - 10} more")
    else:
        print("  [MISSING] No film links found")

    # Special features
    print(f"\n  --- Special Features ---")
    if data.get("special_features"):
        print(f"  Selector: {data.get('special_features_selector')} ({data.get('special_features_count')} items)")
        for feat in data["special_features"][:5]:
            print(f"    - {feat[:100]}")
    else:
        print("  [MISSING] No special features found")

    # OG meta
    if data.get("og_meta"):
        print(f"\n  --- OG Meta Tags ---")
        for k, v in data["og_meta"].items():
            print(f"    {k}: {v[:120]}")

    # Content area classes (for discovering new selectors)
    if data.get("content_area_classes"):
        print(f"\n  --- Content Area: {data.get('content_area_tag', '?')} ---")
        print(f"  CSS classes ({len(data['content_area_classes'])}): {', '.join(data['content_area_classes'][:40])}")

    # Heading structure
    if data.get("heading_structure"):
        print(f"\n  --- Heading Structure ---")
        for h in data["heading_structure"][:15]:
            indent = "  " * (int(h["tag"][1]) - 1)
            parent = f" (parent: .{h['parent_class']})" if h["parent_class"] else ""
            print(f"  {indent}<{h['tag']}> {h['text']}{parent}")


def main():
    scraper = cloudscraper.create_scraper(
        browser={"browser": "chrome", "platform": "darwin", "mobile": False}
    )

    log(f"Exploring {len(SAMPLE_URLS)} box set pages...")

    all_data = []
    for i, (label, url) in enumerate(SAMPLE_URLS):
        log(f"  [{i + 1}/{len(SAMPLE_URLS)}] Fetching {label}...")

        try:
            resp = scraper.get(url, timeout=30)

            if resp.status_code != 200:
                log(f"    HTTP {resp.status_code} -- skipping")
                continue

            if "/shop/browse" in resp.url:
                log(f"    Redirected to browse page -- URL may be stale")
                continue

            soup = BeautifulSoup(resp.text, "html.parser")
            data = extract_data(soup, url)
            all_data.append((label, url, data))
            print_report(label, url, data)

        except Exception as e:
            log(f"    Error: {e}")

        if i < len(SAMPLE_URLS) - 1:
            time.sleep(RATE_LIMIT_SECONDS)

    # --- Summary ---
    print(f"\n\n{'=' * 70}")
    print(f"  SUMMARY: {len(all_data)} / {len(SAMPLE_URLS)} pages fetched")
    print(f"{'=' * 70}")

    field_names = ["title", "year", "director", "description", "image_url",
                   "film_meta_html", "included_films_count", "special_features", "country"]
    print(f"\n  {'Field':<25s} {'Found':>5s} / {'Total':>5s}")
    print(f"  {'-' * 40}")
    for field in field_names:
        found = sum(1 for _, _, d in all_data if d.get(field))
        print(f"  {field:<25s} {found:>5d} / {len(all_data):>5d}")

    # Film counts
    if any(d.get("included_films_count", 0) > 0 for _, _, d in all_data):
        print(f"\n  Film counts per box set:")
        for label, _, d in all_data:
            count = d.get("included_films_count", 0)
            print(f"    {label}: {count} films found")

    log("Done.")


if __name__ == "__main__":
    main()
