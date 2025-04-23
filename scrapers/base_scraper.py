import aiohttp
import asyncio
from database.mongo.mongo_manager import MongoManager
from typing import List, Optional, Dict, Any, Callable
from logger import LoggerManager
import uuid
import time
from datetime import datetime
import re
from bs4 import BeautifulSoup


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
        self.concurrency_limit = 100
        self.session_id = str(uuid.uuid4())
        
        self.logger.info(f"Initializing {scraper_name} with session ID: {self.session_id}")
        if proxies:
            self.logger.info(f"Using {len(proxies)} proxies for requests")
        self.logger.info(f"Concurrency limit set to {self.concurrency_limit}")

    async def _fetch_page_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        headers: Optional[Dict[str, str]] = None,
        timeout: int = 15,
        semaphore: Optional[asyncio.Semaphore] = None,
    ) -> Optional[str]:
        """
        Asynchronously fetches the page content from the given URL.
        Retries on rate limits and client errors up to a maximum number of attempts, with incremental
        backoff. Returns the fetched text if successful, or None otherwise.

        Args:
            session (aiohttp.ClientSession): The session to use for HTTP requests.
            url (str): The target URL.
            headers (Optional[Dict[str, str]]): Optional headers to include in the request.
            timeout (int): The maximum time to await the response.
            semaphore (Optional[asyncio.Semaphore]): Optional semaphore for concurrency control.

        Returns:
            Optional[str]: The HTML content if the fetch is successful, otherwise None.
        """
        start_time = time.time()
        max_retries = 6
        wait_time = 1
        max_wait_time = 15
        
        async def fetch_with_timeout():
            async with asyncio.timeout(timeout):
                return await session.get(url, headers=headers)
        
        self.logger.debug(f"Starting fetch for URL: {url}")
        semaphore_note = "with semaphore" if semaphore else "without semaphore"
        self.logger.debug(f"Fetch configuration: timeout={timeout}s, {semaphore_note}")
        
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.debug(f"[ATTEMPT {attempt}/{max_retries}] Fetching: {url}")
                
                response = None
                if semaphore:
                    async with semaphore:
                        self.logger.debug(f"Acquired semaphore for {url}")
                        response = await fetch_with_timeout()
                else:
                    response = await fetch_with_timeout()
                    
                if response.status in [403, 429]:
                    self.logger.warning(
                        f"[RATE-LIMIT] {response.status} on {url}. Waiting {wait_time}s before retry."
                    )
                    await asyncio.sleep(wait_time)
                    wait_time = min(wait_time * 2, max_wait_time)
                    continue
                    
                if response.status != 200:
                    self.logger.error(f"[HTTP ERROR] Status: {response.status} on {url}")
                    fetch_time = time.time() - start_time
                    self.logger.debug(f"Fetch failed after {fetch_time:.2f}s")
                    return None
                    
                text = await response.text()
                fetch_time = time.time() - start_time
                content_length = len(text)
                self.logger.debug(f"[OK] Successfully fetched {url}: {content_length} bytes in {fetch_time:.2f}s")
                return text
                
            except asyncio.TimeoutError:
                elapsed = time.time() - start_time
                self.logger.error(f"[TIMEOUT] Timeout while fetching {url} after {elapsed:.2f}s")
                if attempt < max_retries:
                    self.logger.info(f"Retrying after timeout ({attempt}/{max_retries})")
                    await asyncio.sleep(wait_time)
                    wait_time = min(wait_time * 2, max_wait_time)
                else:
                    self.logger.error(f"[FAIL] Max retries exceeded for {url} after timeout")
                    return None
                    
            except aiohttp.ClientError as e:
                elapsed = time.time() - start_time
                self.logger.error(f"[CLIENT ERROR] {e} on {url} after {elapsed:.2f}s")
                if attempt < max_retries:
                    self.logger.info(f"Retrying after client error ({attempt}/{max_retries}). Waiting {wait_time}s...")
                    await asyncio.sleep(wait_time)
                    wait_time = min(wait_time * 2, max_wait_time)
                else:
                    self.logger.error(f"[FAIL] Max retries exceeded for {url} after client error")
                    return None
                    
        total_time = time.time() - start_time
        self.logger.error(f"[FAIL] All retry attempts failed for {url} after {total_time:.2f}s")
        return None

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
            self.logger.warning("No URLs provided for multiple fetch operation")
            return []

        start_time = time.time()
        self.logger.info(f"[MULTI] Starting async fetch for {len(urls)} URLs with concurrency limit {self.concurrency_limit}")

        sem = asyncio.Semaphore(self.concurrency_limit)
        tasks = [
            self._fetch_page_async(session, url, headers, semaphore=sem) for url in urls
        ]
        
        self.logger.debug(f"Created {len(tasks)} fetch tasks, waiting for completion")
        results = await asyncio.gather(*tasks)

        successful = sum(1 for r in results if r is not None)
        failed = len(urls) - successful
        success_rate = (successful / len(urls)) * 100 if urls else 0
        fetch_time = time.time() - start_time
        
        self.logger.info(
            f"[MULTI] Fetched {successful}/{len(urls)} pages successfully ({success_rate:.1f}%) in {fetch_time:.2f}s"
        )
        
        if failed > 0:
            self.logger.warning(f"Failed to fetch {failed} URLs")
            
        if fetch_time > 0:
            rate = len(urls) / fetch_time
            self.logger.info(f"Fetch rate: {rate:.2f} URLs/second")
        
        return results

    def _save_data(self, data):
        """
        Save the provided data to MongoDB using the internal manager.

        :param data: The data to be saved.
        """
        if not data:
            self.logger.warning("No data to save.")
            return
            
        start_time = time.time()
        self.logger.info(f"Saving {len(data)} job records to MongoDB")
        
        result = self.mongo_manager.insert_jobs(data)
        save_time = time.time() - start_time
        
        success_rate = (result['success'] / len(data)) * 100 if data else 0
        self.logger.info(
            f"MongoDB save results: {result['success']}/{len(data)} jobs inserted ({success_rate:.1f}%) in {save_time:.2f}s"
        )
        
        if result['invalid_documents'] or result['write_errors']:
            invalid_count = len(result['invalid_documents'])
            error_count = len(result['write_errors'])
            self.logger.warning(
                f"Save issues: {invalid_count} invalid documents, {error_count} write errors"
            )

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
        self.logger.info(f"Starting synchronous scrape of {url}")
        self.logger_manager.log_milestone(f"Starting scraping job for {url}", self.scraper_name)
        
        start_time = time.time()
        try:
            result = asyncio.run(self.scrape_async(url))
            total_time = time.time() - start_time
            
            self.logger.info(f"Scraping completed in {total_time:.2f}s")
            self.logger_manager.log_milestone(f"Scraping job completed in {total_time:.2f}s", self.scraper_name)
            
            return result
        finally:
            self.logger.debug("Closing database connections")
            self.mongo_manager.close()
            self.logger.info("Database connections closed")

    async def load_dynamic_content(
        self,
        url: str,
        button_selector: str,
        content_selector: str,
        max_clicks: int = 30,
        wait_time: int = 2000
    ) -> Optional[str]:
        """
        Uses Playwright to load a page with dynamic content by clicking a "Load More" button
        until all content is loaded or maximum clicks reached.
        
        Args:
            url: The URL to load
            button_selector: CSS selector for the "Load More" button
            content_selector: CSS selector for the content items to count
            max_clicks: Maximum number of button clicks to perform
            wait_time: Milliseconds to wait between clicks
            
        Returns:
            The fully loaded HTML content, or None if failed
        """
        try:
            from playwright.async_api import async_playwright
            
            self.logger.info(f"Starting Playwright to load dynamic content from {url}")
            previous_count = 0
            no_new_content_count = 0
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(viewport={"width": 1920, "height": 1080})
                page = await context.new_page()
                
                await page.goto(url, wait_until="networkidle")
                await page.wait_for_selector(content_selector, timeout=10000)
                
                current_count = await page.locator(content_selector).count()
                self.logger.info(f"Initial content count: {current_count} items")
                
                click_count = 0
                
                while click_count < max_clicks and no_new_content_count < 3:
                    try:
                        is_visible = await page.is_visible(button_selector)
                        if not is_visible:
                            self.logger.info("Load More button no longer visible, all content loaded")
                            break
                            
                        previous_count = await page.locator(content_selector).count()
                        
                        self.logger.debug(f"Clicking load more button (attempt {click_count+1})")
                        await page.click(button_selector)
                        await page.wait_for_timeout(wait_time)
                        await page.wait_for_load_state("networkidle", timeout=5000)
                        click_count += 1
                        
                        current_count = await page.locator(content_selector).count()
                        new_items = current_count - previous_count
                        
                        if new_items > 0:
                            self.logger.info(f"Loaded {new_items} new items (total: {current_count}) after click {click_count}")
                            no_new_content_count = 0
                        else:
                            no_new_content_count += 1
                            self.logger.warning(f"No new content after click {click_count} (still {current_count} items)")
                            
                    except Exception as e:
                        self.logger.warning(f"Error during button click: {str(e)}")
                        try:
                            await page.evaluate(f"document.querySelector('{button_selector}').scrollIntoView()")
                            await page.wait_for_timeout(1000)
                            continue
                        except:
                            self.logger.error(f"Button interaction failed, stopping content loading: {str(e)}")
                            break
                
                content = await page.content()
                final_count = await page.locator(content_selector).count()
                
                await browser.close()
                
                self.logger.info(f"Successfully loaded {final_count} items with {click_count} clicks")
                return content
                
        except ImportError:
            self.logger.error("Playwright not installed. Run: pip install playwright && playwright install chromium")
            return None
        except Exception as e:
            self.logger.error(f"Error loading dynamic content: {str(e)}")
            return None

    async def process_urls_in_batches(
        self,
        session: aiohttp.ClientSession,
        urls: List[str],
        processor_func: Callable[[aiohttp.ClientSession, List[str]], Any],
        batch_size: int = 20,
        operation_name: str = "processing"
    ) -> List[Any]:
        """
        Process a list of URLs in batches with comprehensive progress tracking.
        
        Args:
            session: aiohttp ClientSession for HTTP requests
            urls: List of URLs to process
            processor_func: Async function that processes a batch of URLs
            batch_size: Number of URLs to process in each batch
            operation_name: Name of operation for progress logging
            
        Returns:
            Combined results from all batches
        """
        if not urls:
            self.logger.warning(f"No URLs to process in batch operation")
            return []
            
        total_urls = len(urls)
        total_batches = (total_urls + batch_size - 1) // batch_size
        self.logger.info(f"Processing {total_urls} URLs in {total_batches} batches (size: {batch_size})")
        
        all_results = []
        processed_count = 0
        
        for i in range(0, total_urls, batch_size):
            batch_num = i // batch_size + 1
            batch = urls[i:i + batch_size]
            self.logger.info(f"[BATCH {batch_num}/{total_batches}] Processing {len(batch)} URLs")
            
            batch_start = time.time()
            try:
                batch_results = await processor_func(session, batch)
                if batch_results:
                    all_results.extend(batch_results)
            except Exception as e:
                self.logger.error(f"Error processing batch {batch_num}: {str(e)}")
                continue
                
            batch_time = time.time() - batch_start
            processed_count += len(batch)
            
            self.logger.info(f"Completed batch {batch_num}/{total_batches} in {batch_time:.2f}s")
            
            self.logger_manager.log_progress(processed_count, total_urls, operation_name)
            
            await asyncio.sleep(1)
            
        self.logger.info(f"Completed processing {processed_count}/{total_urls} URLs")
        return all_results

    def _extract_text(self, soup: BeautifulSoup, selector: str) -> Optional[str]:
        """
        Extract text from an element specified by the CSS selector.
        
        :param soup: BeautifulSoup object containing the HTML
        :param selector: CSS selector to find the element
        :return: Text content of the element, or None if not found
        """
        element = soup.select_one(selector)
        if element:
            return element.get_text(strip=True)
        return None
        
    def _normalize_url(self, url: str, base_url: str, region_code: str = None) -> str:
        """
        Normalize a URL to ensure it's an absolute URL with proper formatting.
        
        :param url: The URL to normalize (could be relative or absolute)
        :param base_url: The base URL to use for relative URLs
        :param region_code: Optional region code to insert in the URL path
        :return: Normalized absolute URL
        """
        if not url:
            return ""
            
        if url.startswith("/"):
            has_region = re.match(r'^/[a-z]{2}/', url)
            
            if has_region or not region_code:
                return f"{base_url}{url}"
            else:
                return f"{base_url}/{region_code}{url}"
        
        return url
        
    async def _load_full_page_with_clicks(
        self, 
        url: str, 
        button_selector: str, 
        content_selector: str, 
        max_clicks: int = 30
    ) -> Optional[str]:
        """
        Uses Playwright to load a page with dynamic content by clicking a "Load More" button.
        Tracks content count on page to determine when all content is loaded.
        
        :param url: The URL to load
        :param button_selector: CSS selector for the "Load More" button
        :param content_selector: CSS selector for content items to track
        :param max_clicks: Maximum number of clicks to perform
        :return: Full HTML content after all dynamic content is loaded
        """
        self.logger.info(f"Starting Playwright to load dynamic content from {url}")
        previous_content_count = 0
        
        try:
            from playwright.async_api import async_playwright
            
            async with async_playwright() as p:
                browser = await p.chromium.launch(headless=True)
                context = await browser.new_context(viewport={"width": 1920, "height": 1080})
                page = await context.new_page()
                
                await page.goto(url, wait_until="networkidle")
                
                try:
                    await page.wait_for_selector(content_selector, timeout=30000)
                except Exception as e:
                    self.logger.error(f"Content selector timeout at {url}: {str(e)}")
                    return None
                
                initial_count = await page.locator(content_selector).count()
                self.logger.info(f"Found {initial_count} content items initially")
                
                click_count = 0
                no_new_content_count = 0
                
                while click_count < max_clicks and no_new_content_count < 3:
                    try:
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight * 0.85)")
                        await page.wait_for_timeout(1000)
                        
                        is_visible = await page.is_visible(button_selector)
                        
                        if not is_visible:
                            self.logger.info("Load More button no longer visible, all content loaded")
                            break
                        
                        await page.click(button_selector)
                        self.logger.info(f"Clicked Load More button (click #{click_count+1})")
                        await page.wait_for_timeout(3000)
                        
                        try:
                            await page.wait_for_load_state("networkidle", timeout=8000)
                        except Exception as e:
                            self.logger.warning(f"Network didn't become idle, continuing anyway")
                            await page.wait_for_timeout(2000)
                            
                        click_count += 1
                        
                        new_content_count = await page.locator(content_selector).count()
                        if new_content_count > previous_content_count:
                            self.logger.info(f"Now showing {new_content_count} items (+{new_content_count - previous_content_count}) after {click_count} clicks")
                            previous_content_count = new_content_count
                            no_new_content_count = 0
                        else:
                            no_new_content_count += 1
                            self.logger.warning(f"No new content loaded after click {click_count} (still {new_content_count} items)")
                            
                            if no_new_content_count >= 3:
                                self.logger.info("No new content after multiple attempts, finishing")
                                break
                        
                    except Exception as e:
                        self.logger.warning(f"Error during Load More interaction: {str(e)}")
                        await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
                        await page.wait_for_timeout(2000)
                        continue
                
                content = await page.content()
                final_count = await page.locator(content_selector).count()
                
                await browser.close()
                self.logger.info(f"Successfully loaded {final_count} items with {click_count} clicks")
                
                return content
                
        except Exception as e:
            self.logger.error(f"Playwright error: {str(e)}")
            return None
            
    async def process_batch_with_retries(
        self,
        urls: List[str],
        processor_func: Callable,
        session: aiohttp.ClientSession,
        batch_size: int = 100,
        max_retries: int = 3
    ):
        """
        Process URLs in memory-efficient batches with retry logic for failed requests.
        
        :param urls: List of URLs to process
        :param processor_func: Function to process each URL (should accept session and url)
        :param session: aiohttp.ClientSession to use for HTTP requests
        :param batch_size: Number of URLs to process in each batch
        :param max_retries: Maximum number of retries for failed requests
        :return: Dict with counts of processed, saved, and failed items
        """
        total_batches = (len(urls) + batch_size - 1) // batch_size
        self.logger.info(f"Processing {len(urls)} items in {total_batches} batches (size: {batch_size})")
        
        total_processed = 0
        total_saved = 0
        total_failed = 0
        retry_urls = []
        
        for i in range(0, len(urls), batch_size):
            batch = urls[i:i + batch_size]
            batch_num = i // batch_size + 1
            
            self.logger.info(f"[BATCH {batch_num}/{total_batches}] Processing {len(batch)} items")
            
            batch_results = await processor_func(session, batch)
            
            if batch_results:
                if 'processed' in batch_results:
                    total_processed += batch_results['processed']
                if 'saved' in batch_results:
                    total_saved += batch_results['saved']
                if 'failed' in batch_results:
                    total_failed += batch_results['failed']
                if 'retry_urls' in batch_results and batch_results['retry_urls']:
                    retry_urls.extend(batch_results['retry_urls'])
            
            progress = (total_processed / len(urls)) * 100
            self.logger.info(f"Progress: {progress:.1f}% complete ({total_processed}/{len(urls)})")
            
            await asyncio.sleep(2)
        
        if retry_urls and max_retries > 0:
            self.logger.info(f"Retrying {len(retry_urls)} failed items (retries left: {max_retries})")
            retry_results = await self.process_batch_with_retries(
                retry_urls, processor_func, session, batch_size, max_retries - 1
            )
            
            if retry_results:
                total_processed += retry_results.get('processed', 0)
                total_saved += retry_results.get('saved', 0)
                total_failed = retry_results.get('failed', 0)
        elif retry_urls:
            total_failed = len(retry_urls)
            self.logger.warning(f"Failed to process {total_failed} items after all retry attempts")
        
        self.logger.info(f"Completed processing with {total_saved}/{len(urls)} items saved, {total_failed} failed")
        
        return {
            'processed': total_processed,
            'saved': total_saved,
            'failed': total_failed
        }
