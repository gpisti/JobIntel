import aiohttp
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from scrapers.base_scraper import BaseScraper
import time


class ProfessionScraper(BaseScraper):
    """A specialized scraper for Profession.hu job listings, extending BaseScraper to handle asynchronous page fetching, logging, data parsing, and storage.
    Provides methods to traverse multiple pages, extract job details from HTML content, and save retrieved data in batches.
    """
    CSS_SELECTORS = {
        "job_count": "#jobs_block_count > div",
        "job_links": "div.card-footer.bottom span.actions > a, div.job-card a",
        "title": "#job-title",
        "company":  "#main > div:nth-child(1) > div > div.adv-cover-wrapper > div.adv-cover > div > div > div > section > ul > li:nth-child(1) > div > h2",
        "location": "#main > div:nth-child(1) > div > div.adv-cover-wrapper > div.adv-cover > div > div > div > section > ul > li:nth-child(2) > div > div.my-auto > h2",
        "description": "#content > div > div:nth-child(1) > div > div > div.wrap > div > div > section",
    }

    def __init__(
        self,
        scraper_name: str = "ProfessionScraper",
        proxies: Optional[List[str]] = None,
        log_dir: str = "logs",
    ):
        """
        Initialize the ProfessionScraper with the given scraper name, optional proxies, and a log directory.

        Sets the default base URL to 'https://www.profession.hu' and the number of jobs per page to 20.
        """
        super().__init__(scraper_name, proxies, log_dir)
        self.base_url: str = "https://www.profession.hu"
        self.jobs_per_page: int = 20
        self.logger.info(f"ProfessionScraper initialized with base URL: {self.base_url}")
        self.logger.info(f"Using session ID: {self.session_id}")
        self.logger_manager.log_milestone("New scraping session started", "profession_scraper")

    async def scrape_async(self, start_url: str):
        """
        Asynchronously scrapes job listings starting from the given URL, collects and processes job links in batches, and stores the extracted data.

        :param start_url: The initial URL to begin scraping from.
        """
        start_time = time.time()
        self.logger.info(f"[SCRAPE START] Starting scraping from URL: {start_url}")
        self.logger_manager.log_milestone(f"Starting scraping from {start_url}", "profession_scraper")

        async with aiohttp.ClientSession() as session:
            self.logger.info("Fetching initial page to determine job count")
            first_page_html = await self._fetch_page_async(session, start_url)
            if not first_page_html:
                self.logger.error("[ERROR] Could not fetch the first page. Aborting scraping.")
                self.logger_manager.log_milestone("Scraping aborted: Failed to fetch first page", "profession_scraper")
                return

            self.logger.info("Extracting job count and calculating total pages")
            total_jobs, total_pages = self._extract_job_count(first_page_html)
            if not total_jobs:
                self.logger.error("[ERROR] Could not determine job count. Aborting scraping.")
                self.logger_manager.log_milestone("Scraping aborted: Failed to determine job count", "profession_scraper")
                return
                
            self.logger.info(f"[INFO] Found {total_jobs} jobs across {total_pages} pages")
            self.logger_manager.log_milestone(f"Found {total_jobs} jobs to process", "profession_scraper")

            self.logger.info(f"Generating URLs for {total_pages} pages")
            page_urls = [f"{self.base_url}/allasok/{i}" for i in range(1, total_pages + 1)]
            self.logger.info(f"Fetching {len(page_urls)} listing pages")
            fetch_start = time.time()
            pages_html = await self._fetch_multiple_pages_async(session, page_urls)
            fetch_time = time.time() - fetch_start
            self.logger.info(f"Fetched {len(pages_html)} listing pages in {fetch_time:.2f}s")

            extract_start = time.time()
            self.logger.info("Extracting job URLs from all listing pages")
            job_urls = self._extract_job_urls(pages_html)
            extract_time = time.time() - extract_start
            self.logger.info(f"[JOB URLS] Extracted {len(job_urls)} job links in {extract_time:.2f}s")
            self.logger_manager.log_milestone(f"Extracted {len(job_urls)} job links", "profession_scraper")

            self.logger.info("Starting batch processing of job details")
            await self._process_job_batches(session, job_urls)
            
            total_time = time.time() - start_time
            self.logger.info(f"[SCRAPE DONE] Completed scraping in {total_time:.2f}s")
            self.logger_manager.log_milestone(f"Scraping session completed", "profession_scraper")

    def _extract_job_count(self, html_content: str) -> tuple:
        """Extract the total number of jobs and calculate number of pages."""
        self.logger.debug("Parsing job count from HTML content")
        soup = BeautifulSoup(html_content, "html.parser")
        job_count_div = soup.select_one(self.CSS_SELECTORS["job_count"])
        
        if not job_count_div:
            self.logger.error("Could not find job count element in HTML")
            return 0, 0
            
        job_count_text = job_count_div.get_text(strip=True).split(" ")[0].replace(".", "")
        try:
            total_jobs = int(job_count_text)
            total_pages = (total_jobs + self.jobs_per_page - 1) // self.jobs_per_page
            self.logger.debug(f"Successfully parsed job count: {total_jobs} jobs, {total_pages} pages")
            return total_jobs, total_pages
        except ValueError as e:
            self.logger.error(f"Failed to parse job count from text: '{job_count_text}'. Error: {str(e)}")
            return 0, 0

    def _extract_job_urls(self, pages_html: List[Optional[str]]) -> List[str]:
        """Extract job URLs from multiple pages using BaseScraper's URL normalization."""
        self.logger.debug(f"Extracting job URLs from {len(pages_html)} HTML pages")
        job_urls = []
        empty_pages = 0
        
        for i, html in enumerate(pages_html):
            if not html:
                empty_pages += 1
                continue
                
            soup = BeautifulSoup(html, "html.parser")
            job_links = soup.select(self.CSS_SELECTORS["job_links"])
            
            page_urls = 0
            for link in job_links:
                href = link.get("href")
                if href:
                    normalized_url = self._normalize_url(href, self.base_url)
                    job_urls.append(normalized_url)
                    page_urls += 1
                    
            self.logger.debug(f"Page {i+1}: Found {page_urls} job links")
        
        if empty_pages > 0:
            self.logger.warning(f"Found {empty_pages} empty pages during URL extraction")
            
        self.logger.info(f"Extracted {len(job_urls)} total job URLs from {len(pages_html) - empty_pages} pages")
        return job_urls

    async def _process_job_batches(self, session, job_urls, batch_size=1000):
        """
        Process job URLs in batches using BaseScraper's batch processing with retries.
        """
        if not job_urls:
            self.logger.warning("No job URLs to process")
            return
            
        async def process_batch(session, batch):
            self.logger.info(f"Processing batch of {len(batch)} job URLs")
            
            batch_start = time.time()
            job_details_pages = await self._fetch_multiple_pages_async(session, batch)
            fetch_time = time.time() - batch_start
            
            success_count = sum(1 for page in job_details_pages if page is not None)
            self.logger.info(f"Fetched {success_count}/{len(batch)} job details in {fetch_time:.2f}s")
            
            extract_start = time.time()
            self.logger.info("Extracting job data from fetched HTML")
            job_data = []
            processed = 0
            failed = 0
            
            for j, html_content in enumerate(job_details_pages):
                processed += 1
                if html_content:
                    job_url = batch[j]
                    details = self.extract_job_details(html_content, job_url)
                    if details:
                        job_data.append(details)
                    else:
                        self.logger.warning(f"Failed to extract job details from {job_url}")
                        failed += 1
                else:
                    failed += 1
            
            extract_time = time.time() - extract_start
            self.logger.info(f"Extracted {len(job_data)} job records in {extract_time:.2f}s")
            
            self._save_data(job_data)
            
            return {
                'processed': processed,
                'saved': len(job_data),
                'failed': failed
            }
            
        results = await self.process_batch_with_retries(
            job_urls, process_batch, session, batch_size, max_retries=3
        )
        
        return results

    def extract_job_details(
        self, html_content: str, job_url: str
    ) -> Dict[str, Optional[str]]:
        """
        Extracts job information such as title, company, location, and description from the given HTML content.

        :param html_content: The raw HTML content of the job listing.
        :param job_url: The URL of the job listing.
        :return: A dictionary containing the extracted job details.
        """
        self.logger.debug(f"Extracting job details from {job_url}")
        soup = BeautifulSoup(html_content, "html.parser")

        title = self._extract_text(soup, self.CSS_SELECTORS["title"])
        company = self._extract_text(soup, self.CSS_SELECTORS["company"])
        location = self._extract_text(soup, self.CSS_SELECTORS["location"])
        description = self._extract_text(soup, self.CSS_SELECTORS["description"])
        
        if not (title and company and location and description):
            missing = []
            if not title: missing.append("title")
            if not company: missing.append("company")
            if not location: missing.append("location")
            if not description: missing.append("description")
            self.logger.warning(f"Missing fields in job {job_url}: {', '.join(missing)}")
        
        job_data = {
            "title": title,
            "company": company,
            "location": location,
            "job_url": job_url,
            "description": description,
            "source_platform": "Profession.hu",
            "session_id": self.session_id,
        }
        
        return job_data


if __name__ == "__main__":
    scraper = ProfessionScraper()
    scraper.scrape("https://www.profession.hu/allasok")
