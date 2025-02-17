import aiohttp
import asyncio
import json
import datetime
from typing import List, Optional, Dict, Any
from logger import LoggerManager


class BaseScraper:
    """
    Provides an asynchronous scraping base class with caching, rate-limited retries,
    and concurrency control. Subclasses should override scrape_async() to add
    custom scraping logic. This class can store data in JSON, with plans to support
    saving data to a database in a future release.
    """

    def __init__(
        self,
        scraper_name: str,
        proxies: Optional[List[str]] = None,
        log_dir: str = "logs",
    ):
        self.scraper_name = scraper_name
        self.logger_manager = LoggerManager(log_dir=log_dir)
        self.logger = self.logger_manager.get_logger(scraper_name)
        self.proxies = proxies

        self.visited_urls: set = set()
        self.cached_pages: dict = {}

        self.concurrency_limit: int = 100

    async def _fetch_page_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 15,
    ) -> Optional[str]:
        """
        Fetches the specified URL asynchronously using an internal cache and exponential backoff.

        Parameters:
            session (aiohttp.ClientSession): The HTTP session for making requests.
            url (str): The URL to request.
            headers (Optional[Dict[str, str]]): Additional HTTP headers.
            timeout (int): The request timeout, in seconds.

        Returns:
            Optional[str]: The response text if successful, otherwise None.
        """
        if url in self.visited_urls:
            self.logger.debug(f"[CACHE HIT] Already visited: {url}")
            return self.cached_pages.get(url)

        max_retries = 6
        wait_time = 1
        max_wait_time = 15

        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(f"[ATTEMPT {attempt}] Fetching: {url}")

                async with asyncio.timeout(timeout):
                    async with session.get(url, headers=headers) as response:
                        if response.status in [403, 429]:
                            self.logger.warning(
                                f"[RATE-LIMIT] {response.status} on {url}. Waiting {wait_time}s before retry."
                            )
                            await asyncio.sleep(wait_time)
                            wait_time = min(wait_time * 2, max_wait_time)
                            continue

                        if response.status != 200:
                            self.logger.error(
                                f"[HTTP ERROR] Status: {response.status} on {url}"
                            )
                            return None

                        text = await response.text()
                        self.visited_urls.add(url)
                        self.cached_pages[url] = text
                        self.logger.info(f"[OK] Asynchronously fetched page: {url}")
                        return text

            except asyncio.TimeoutError:
                self.logger.error(f"[TIMEOUT] Timeout while fetching {url}, skipping.")
                return None
            except aiohttp.ClientError as e:
                self.logger.error(
                    f"[EXCEPTION] {e} on {url}. Retrying in {wait_time}s..."
                )
                await asyncio.sleep(wait_time)
                wait_time = min(wait_time * 2, max_wait_time)

        self.logger.error(f"[FAIL] Max retries exceeded for {url}")
        return None

    async def _limited_fetch_page_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        sem: asyncio.Semaphore,
        headers: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """
        Fetches a page using an async request with concurrency control.

        Args:
            session (aiohttp.ClientSession): The HTTP session to use for the request.
            url (str): The target URL.
            sem (asyncio.Semaphore): The semaphore limiting concurrency.
            headers (Optional[Dict[str, str]]): Additional request headers.

        Returns:
            Optional[str]: The response text if successfully fetched, otherwise None.
        """
        async with sem:
            return await self._fetch_page_async(session, url, headers)

    async def _fetch_multiple_pages_async(
        self,
        session: aiohttp.ClientSession,
        urls: List[str],
        headers: Optional[Dict[str, str]] = None,
    ) -> List[Optional[str]]:
        """
        Fetches multiple URLs concurrently, respecting a concurrency limit.

        Args:
            session (aiohttp.ClientSession): The HTTP session used for the requests.
            urls (List[str]): The list of URLs to be fetched.
            headers (Optional[Dict[str, str]]): Additional headers for the requests.

        Returns:
            List[Optional[str]]: The content of each successfully fetched page, or None if it failed.
        """
        if not urls:
            return []

        self.logger.debug(f"[MULTI] Starting async fetch for {len(urls)} URLs...")

        sem = asyncio.Semaphore(self.concurrency_limit)
        tasks = [
            self._limited_fetch_page_async(session, url, sem, headers) for url in urls
        ]
        results = await asyncio.gather(*tasks)

        final_results = []
        for url, res in zip(urls, results):
            if res is None:
                self.logger.error(f"[ERROR] Failed to fetch {url}")
                final_results.append(None)
            else:
                final_results.append(res)

        self.logger.info(f"[MULTI] Fetched {len(final_results)}/{len(urls)} pages.")
        return final_results

    def _save_data(
        self,
        data: List[Dict[str, Any]],
        filename: Optional[str] = None,
        format: str = "json",
    ):
        """
        Save the provided data to a JSON file with a time-stamped filename if none is specified.

        Parameters
        ----------
        data : List[Dict[str, Any]]
            Data to be saved.
        filename : Optional[str]
            Optional custom filename.
        format : str
            Desired file format (default: "json").

        Note
        ----
        This implementation is only temporary and will be modified to store data in a database instead of JSON files.
        """
        if not filename:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{self.scraper_name}_data_{timestamp}.json"
        if format == "json":
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)

    async def scrape_async(self, url: str):
        """
        Scrapes data asynchronously from the specified URL.

        :param url: The target URL to scrape.
        :raises NotImplementedError: If the method is not overridden by a subclass.
        """
        raise NotImplementedError("Subclasses must implement this method (async).")

    def scrape(self, url: str):
        """
        Synchronous interface to the asynchronous scraping logic.

        :param url: The target URL to scrape.
        :return: The result of the asynchronous scraping operation.
        """
        return asyncio.run(self.scrape_async(url))
