import aiohttp
import asyncio
from database.mongo.mongo_manager import MongoManager
from typing import List, Optional, Dict
from logger import LoggerManager
import uuid


class BaseScraper:
    """
    Base class for async scraping with retry, rate limiting, and concurrency control.
    Child scrapers should override `scrape_async()`.
    """

    def __init__(
        self,
        scraper_name: str,
        proxies: Optional[List[str]] = None,
        log_dir: str = "logs",
    ):
        """
        Initialize the scraper with a logger, MongoDB manager, and optional proxies.

        Args:
            scraper_name (str): Unique name for the scraper.
            proxies (Optional[List[str]]): Optional list of proxy URLs to use.
            log_dir (str): Directory where log files are stored. Defaults to "logs".
        """
        self.scraper_name = scraper_name

        self.logger_manager = LoggerManager(log_dir=log_dir)
        self.logger = self.logger_manager.get_logger(scraper_name)

        self.mongo_manager = MongoManager(self.logger_manager)

        self.proxies = proxies
        self.visited_urls: set = set()
        self.cached_pages: dict = {}
        self.concurrency_limit: int = 100
        self.session_id = str(uuid.uuid4())

    async def _fetch_page_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 15,
    ) -> Optional[str]:
        """
        Asynchronously fetches the page content from the given URL, caching successful responses.
        Retries on rate limits and client errors up to a maximum number of attempts, with incremental
        backoff. Returns the fetched text if successful, or None otherwise.

        Args:
            session (aiohttp.ClientSession): The session to use for HTTP requests.
            url (str): The target URL.
            headers (Optional[Dict[str, str]]): Optional headers to include in the request.
            timeout (int): The maximum time to await the response.

        Returns:
            Optional[str]: The HTML content if the fetch is successful, otherwise None.
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
        Acquires the provided concurrency-limiting semaphore before asynchronously
        fetching the specified URL. Once the semaphore is secured, this method
        delegates the work to _fetch_page_async.

        Args:
            session (aiohttp.ClientSession): The session used for HTTP requests.
            url (str): The target URL.
            sem (asyncio.Semaphore): Controls concurrency.
            headers (Optional[Dict[str, str]]): Optional request headers.

        Returns:
            Optional[str]: The page content, or None if fetching fails.
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
        Asynchronously fetches multiple URLs using a limited concurrency approach.

        Args:
            session (aiohttp.ClientSession): The session used for sending requests.
            urls (List[str]): The list of URLs to fetch.
            headers (Optional[Dict[str, str]]): Additional headers for the requests.

        Returns:
            List[Optional[str]]: The HTML content of each URL, or None for failed fetches.
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

    def _save_data(self, data):
        """
        Save the provided data to MongoDB using the internal manager.

        :param data: The data to be saved.
        """
        if data:
            self.mongo_manager.insert_jobs(data)
            self.logger.info(f"Saved {len(data)} jobs to MongoDB.")
        else:
            self.logger.warning("No data to save.")

    async def scrape_async(self, url: str):
        """
        Asynchronously scrape data from the specified URL.

        Args:
            url (str): The URL to scrape.

        Raises:
            NotImplementedError: Indicates that subclasses must implement this method.
        """
        raise NotImplementedError("Subclasses must implement this method (async).")

    def scrape(self, url: str):
        """
        Synchronously scrape the specified URL by invoking the asynchronous scraping method.

        :param url: The URL to scrape.
        :return: The result from the asynchronous scraping operation.
        """
        return asyncio.run(self.scrape_async(url))
