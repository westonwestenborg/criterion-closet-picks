"""
Playwright-based browser for scraping Criterion.com.
Replaces cloudscraper to bypass Cloudflare Managed Challenge (Turnstile).
"""

from dataclasses import dataclass

from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page
from playwright_stealth import Stealth


@dataclass
class FetchResult:
    """Mimics the subset of requests.Response used by scraping scripts."""

    status_code: int
    text: str
    url: str


class CriterionBrowser:
    """
    Context manager wrapping a stealth Playwright browser.

    Usage:
        with CriterionBrowser() as browser:
            result = browser.fetch("https://www.criterion.com/...", timeout=30)
            # result.status_code, result.text, result.url
    """

    def __init__(self):
        self._stealth = Stealth()
        self._pw_cm = None
        self._pw: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    def __enter__(self):
        self._pw_cm = self._stealth.use_sync(sync_playwright())
        self._pw = self._pw_cm.__enter__()
        self._browser = self._pw.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
        )
        self._page = self._context.new_page()
        return self

    def __exit__(self, *exc):
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()
        if self._pw_cm:
            self._pw_cm.__exit__(*exc)

    def fetch(self, url: str, timeout: int = 30) -> FetchResult:
        """
        Navigate to url and return a FetchResult with status_code, text, and final url.
        timeout is in seconds (converted to ms for Playwright).
        """
        response = self._page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=timeout * 1000,
        )
        status = response.status if response else 0
        final_url = self._page.url
        html = self._page.content()
        return FetchResult(status_code=status, text=html, url=final_url)
