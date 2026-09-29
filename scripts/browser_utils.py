"""
Playwright-based browser for scraping Criterion.com.
Replaces cloudscraper to bypass Cloudflare Managed Challenge (Turnstile).
"""

import re
from dataclasses import dataclass

from playwright.sync_api import sync_playwright, Playwright, Browser, BrowserContext, Page
from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright_stealth import Stealth

from scripts.utils import log

# Cloudflare's interstitial. A managed challenge answers 403, and its title is
# the same whatever status it comes with.
_CHALLENGE_TITLE = re.compile(r"<title>\s*Just a moment", re.I)

# How long a challenge gets to clear on its own before a browser counts as stuck.
CHALLENGE_WAIT_SECONDS = 15


@dataclass
class FetchResult:
    """Mimics the subset of requests.Response used by scraping scripts."""

    status_code: int
    text: str
    url: str

    @property
    def blocked(self) -> bool:
        """True when Cloudflare served its challenge page instead of the real one."""
        return self.status_code == 403 or bool(_CHALLENGE_TITLE.search(self.text))


class CriterionBrowser:
    """
    Context manager wrapping a stealth Playwright browser.

    Starts headless. If Cloudflare holds a page on its challenge, the browser
    reopens as a visible Chrome window and stays visible for the rest of the
    session. A result that is still blocked after that has `blocked` set, and
    what to do about it is the caller's call.

    Usage:
        with CriterionBrowser() as browser:
            result = browser.fetch("https://www.criterion.com/...", timeout=30)
            # result.status_code, result.text, result.url, result.blocked
    """

    def __init__(self):
        self._stealth = Stealth()
        self._pw_cm = None
        self._pw: Playwright | None = None
        self._browser: Browser | None = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None
        self._headless = True

    def __enter__(self):
        self._pw_cm = self._stealth.use_sync(sync_playwright())
        self._pw = self._pw_cm.__enter__()
        self._launch()
        return self

    def __exit__(self, *exc):
        self._close_browser()
        if self._pw_cm:
            self._pw_cm.__exit__(*exc)

    def _launch(self) -> None:
        # Installed Google Chrome, not Playwright's bundled Chromium. On the
        # morning of 2026-09-29 Cloudflare held the bundled build on "Just a
        # moment..." (headless and headed, 30s+ each) while Chrome passed on
        # its first request. The bundled build passed again that afternoon, so
        # blocks come and go; Chrome is the one that got through.
        self._browser = self._pw.chromium.launch(
            channel="chrome",
            headless=self._headless,
            args=["--disable-blink-features=AutomationControlled"],
        )
        self._context = self._browser.new_context(
            viewport={"width": 1920, "height": 1080},
        )
        self._page = self._context.new_page()

    def _close_browser(self) -> None:
        if self._context:
            self._context.close()
        if self._browser:
            self._browser.close()

    def fetch(self, url: str, timeout: int = 30) -> FetchResult:
        """
        Navigate to url and return a FetchResult with status_code, text, and final url.
        timeout is in seconds (converted to ms for Playwright).
        """
        result = self._fetch_once(url, timeout)
        if result.blocked and self._headless:
            # Headless is the easier mode for Cloudflare to flag, so a visible
            # window is the next thing to try. On 2026-09-29 a visible Chrome
            # window got a 200 on its first request.
            log("  Cloudflare challenge in headless Chrome; retrying in a visible window")
            self._close_browser()
            self._headless = False
            self._launch()
            result = self._fetch_once(url, timeout)
        return result

    def _fetch_once(self, url: str, timeout: int) -> FetchResult:
        result = self._goto(url, timeout)
        if not result.blocked:
            return result
        # A challenge can clear itself after a few seconds of JS. Once it has,
        # Cloudflare has set its clearance cookie, so load the page again to get
        # the real response and its status.
        try:
            self._page.wait_for_function(
                "!document.title.startsWith('Just a moment')",
                timeout=CHALLENGE_WAIT_SECONDS * 1000,
            )
        except PlaywrightTimeoutError:
            return result
        return self._goto(url, timeout)

    def _goto(self, url: str, timeout: int) -> FetchResult:
        response = self._page.goto(
            url,
            wait_until="domcontentloaded",
            timeout=timeout * 1000,
        )
        status = response.status if response else 0
        final_url = self._page.url
        html = self._page.content()
        return FetchResult(status_code=status, text=html, url=final_url)
