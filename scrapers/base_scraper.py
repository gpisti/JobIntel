import requests
from bs4 import BeautifulSoup
import time
import random
import json
import datetime
from typing import List, Optional, Dict, Any
from lxml import html
from playwright.sync_api import sync_playwright
from logger import LoggerManager


class BaseScraper:
    def __init__(
        self,
        scraper_name: str,
        proxies: Optional[List[str]] = None,
        log_dir: str = "logs",
    ):
        self.scraper_name = scraper_name
        self.logger_manager = LoggerManager(log_dir=log_dir)
        self.logger = self.logger_manager.get_logger(scraper_name)
        self.session = requests.Session()
        self.proxies = proxies
        self.current_proxy = None
        self.proxy_fail_count = {}

    def _fetch_page(
        self, url: str, headers: Optional[Dict[str, str]] = None
    ) -> Optional[str]:
        """Fetch a web page with retry and proxy support."""
        for attempt in range(5):
            try:
                proxy = self._get_proxy()
                proxies = {"http": proxy, "https": proxy} if proxy else None

                response = self.session.get(
                    url, headers=headers, proxies=proxies, timeout=10
                )
                response.raise_for_status()

                response.encoding = response.apparent_encoding
                self.logger.info(f"Fetching page: {url}")
                return response.text
            except requests.exceptions.HTTPError as e:
                if response.status_code in [403, 429]:
                    wait_time = 2**attempt
                    self.logger.warning(
                        f"Request failed ({response.status_code} {response.reason}). Retrying in {wait_time} seconds."
                    )
                    time.sleep(wait_time)
                    if proxy:
                        self._handle_proxy_failure(proxy)
                else:
                    self.logger.error(f"HTTP Error: {e}")
                    return None
            except requests.exceptions.RequestException as e:
                self.logger.error(f"Request Exception: {e}")
                return None
        return None

    def _get_proxy(self) -> Optional[str]:
        """Get a random working proxy, removing failed ones."""
        if not self.proxies:
            return None
        if self.current_proxy and self.proxy_fail_count.get(self.current_proxy, 0) < 3:
            return self.current_proxy
        self.current_proxy = random.choice(self.proxies)
        return self.current_proxy

    def _handle_proxy_failure(self, proxy: str):
        """Increase failure count for proxies and remove them if necessary."""
        self.proxy_fail_count[proxy] = self.proxy_fail_count.get(proxy, 0) + 1
        if self.proxy_fail_count[proxy] >= 3:
            self.proxies.remove(proxy)
            self.logger.warning(
                f"Proxy {proxy} removed from rotation due to repeated failures."
            )

    def _random_delay(self):
        """Introduce a random delay to avoid detection."""
        delay = random.uniform(2, 5)
        time.sleep(delay)

    def _extract_data(
        self, html_content: str, css_selector: str, xpath: Optional[str] = None
    ) -> List[str]:
        """Extract data from the HTML using either CSS selectors or XPath."""
        if xpath:
            tree = html.fromstring(html_content)
            return tree.xpath(xpath)
        soup = BeautifulSoup(html_content, "html.parser")
        elements = soup.select(css_selector)
        return [element.get_text(strip=True) for element in elements]

    def _normalize_data(self, data: List[str]) -> List[str]:
        """Normalize extracted data (strip spaces, fix encodings)."""
        return [text.strip() for text in data]

    def get_next_page(
        self, html_content: str, next_button_selector: str = "a.next-page"
    ) -> Optional[str]:
        """Determine the next page URL using a customizable selector."""
        soup = BeautifulSoup(html_content, "html.parser")
        next_button = soup.select_one(next_button_selector)
        return next_button["href"] if next_button else None

    def _save_data(self, data: List[Dict[str, Any]], format: str = "json"):
        """Save data to a file (default: JSON) with timestamped filename."""
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.scraper_name}_data_{timestamp}.json"
        if format == "json":
            with open(filename, "w") as f:
                json.dump(data, f, indent=4)

    def enable_playwright(self):
        """Enable Playwright for handling JavaScript-rendered pages."""
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(headless=True)
        self.page = self.browser.new_page()

    def close_playwright(self):
        """Close Playwright instances."""
        if hasattr(self, "browser"):
            self.browser.close()
        if hasattr(self, "playwright"):
            self.playwright.stop()

    def scrape(self, url: str, headers: Optional[Dict[str, str]] = None):
        """Placeholder method. To be implemented in subclasses."""
        raise NotImplementedError("Subclasses must implement this method.")
