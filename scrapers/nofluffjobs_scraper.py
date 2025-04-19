import aiohttp
from typing import List, Dict, Optional, Any
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import asyncio
import time
from datetime import datetime
from scrapers.base_scraper import BaseScraper
import re


class NoFluffJobsScraper(BaseScraper):
    """
    Scraper for NoFluffJobs job listings, focused on raw data extraction for ETL pipeline.
    Extracts job data from dynamic pages and stores completely raw data in MongoDB
    for later transformation and loading to PostgreSQL.
    """

    CSS_SELECTORS = { 
        "load_more_button": "body > nfj-root > nfj-layout > nfj-main-content > div > nfj-postings-search > div > div.tw-mb-5.tw-mt-3.lg\:tw-mt-6.container > div > div > common-main-loader > div > div > nfj-homepage-listings > div.tw-flex.tw-justify-center.tw-mt-1\.5.tw-mb-16 > button",
        "job_cards": "a[href*='/job/']",
        
        "title": "#posting-header > div > div > h1",
        "company": "#postingCompanyUrl",
        "all_specs": "[data-cy^='posting-specs-']", 
        "description": ".posting-content",
        "all_sections": ".posting-details section", 
        
        "requirements": "#posting-requirements ul",
        "nice_to_have": "#posting-nice-to-have ul",
        "textual_requirements": "section.posting-requirements",
        "short_description": "#posting-description",
        "daily_tasks": "#posting-tasks",
        "specifications": "#posting-specs",
        "office_benefits": "#posting-benefits > section.purple",
        "extra_benefits": "#posting-benefits > section.success",
        "methodology": "#posting-environment",
        "provided_equipment": "#posting-equipment ul",
        "location": "common-posting-locations span span:nth-child(1)",
        "work_location": "#posting-header > div.mobile-list.tw-mt-4.ng-star-inserted > div.tw-h-12.border-bottom.tw-flex.tw-items-center.ng-star-inserted > common-posting-locations > div > span > span:nth-child(1)",
        "seniority_level": "#posting-seniority > div > span",
        
        "benefits": ".benefits-list li",
        "apply_button": "a.application-button",
    }

    def __init__(
        self,
        scraper_name: str = "NoFluffJobsScraper",
        proxies: Optional[List[str]] = None,
        log_dir: str = "logs",
    ):
        """
        Initialize the NoFluffJobsScraper with the given scraper name, optional proxies, and a log directory.
        """
        super().__init__(scraper_name, proxies, log_dir)
        self.base_url: str = "https://nofluffjobs.com/hu"
        self.logger.info(f"NoFluffJobsScraper initialized with base URL: {self.base_url}")
        self.logger.info(f"Using session ID: {self.session_id}")
        self.logger_manager.log_milestone("NoFluffJobs extraction session started", "etl_pipeline")
        
    async def scrape_async(self, start_url: str):
        """
        Asynchronously scrapes raw job data from NoFluffJobs using a three-phase approach:
        1. Load the main page with all job listings visible (expand all)
        2. Extract all job URLs from the fully loaded page
        3. Process the job URLs in memory-efficient batches
        
        :param start_url: The URL to scrape (e.g., "https://nofluffjobs.com/hu/jobs")
        """
        start_time = time.time()
        self.logger.info(f"[EXTRACTION START] URL: {start_url}")
        self.logger_manager.log_milestone(f"Starting extraction from {start_url}", "etl_pipeline")
        
        region_match = re.search(r'nofluffjobs\.com/([a-z]{2})/', start_url)
        region_code = region_match.group(1) if region_match else None
        if region_code:
            self.logger.info(f"Detected region code: {region_code}")
            self.base_url = f"https://nofluffjobs.com/{region_code}"
            self.logger.info(f"Updated base URL to: {self.base_url}")
        else:
            self.logger.warning("No region code detected in URL, using default base URL")
        
        self.logger.info("PHASE 1: Loading full main page with all job listings")
        try:
            full_html = await self._load_full_page_with_clicks(
                start_url, 
                self.CSS_SELECTORS["load_more_button"],
                self.CSS_SELECTORS["job_cards"],
                max_clicks=30
            )
            
            if not full_html:
                self.logger.error("Failed to load dynamic content. Extraction aborted.")
                return
                
            timestamp = datetime.now().isoformat()
            listing_record = {
                "url": start_url,
                "html": full_html,
                "timestamp": timestamp,
                "session_id": self.session_id,
                "type": "listing_page"
            }
            self._save_data([listing_record])
            self.logger.info(f"Saved listing page snapshot ({len(full_html)} bytes)")
            
        except Exception as e:
            self.logger.error(f"Critical error loading dynamic content: {str(e)}")
            return
        
        self.logger.info("PHASE 2: Extracting all job URLs from the fully loaded page")
        job_urls = self._extract_job_urls(full_html)
        if not job_urls:
            self.logger.error("No job URLs found. Extraction aborted.")
            return
            
        self.logger.info(f"Found {len(job_urls)} job URLs to process")
        self.logger_manager.log_milestone(f"Extracting {len(job_urls)} job listings", "etl_pipeline")
        
        full_html = None
        
        self.logger.info("PHASE 3: Processing job URLs in memory-efficient batches")
        await self._process_job_batches(job_urls, batch_size=250)
        
        total_time = time.time() - start_time
        self.logger.info(f"[EXTRACTION COMPLETE] Processed {len(job_urls)} jobs in {total_time:.2f}s")
        self.logger_manager.log_milestone(f"Completed extraction of {len(job_urls)} NoFluffJobs listings", "etl_pipeline")
        
    async def _load_full_page_with_clicks(self, url: str, button_selector: str, job_card_selector: str, max_clicks: int = 30) -> Optional[str]:
        """
        Uses Playwright to load a page with dynamic content by clicking a "Load More" button.
        Uses BaseScraper implementation.
        
        :return: Full HTML content after all dynamic content is loaded
        """
        return await super()._load_full_page_with_clicks(url, button_selector, job_card_selector, max_clicks)
            
    def _extract_job_urls(self, html_content: str) -> List[str]:
        """
        Extract all job URLs from the listing page HTML using href attributes.
        Ensures all URLs are valid and absolute with correct region code.
        
        :param html_content: HTML content of the listing page
        :return: List of absolute job URLs
        """
        if not html_content:
            self.logger.error("No HTML content provided for URL extraction")
            return []
            
        region_match = re.search(r'nofluffjobs\.com/([a-z]{2})', self.base_url)
        region_code = region_match.group(1) if region_match else ''
        self.logger.info(f"Using region code '{region_code}' for URL normalization")
        
        job_urls = set()
        try:
            soup = BeautifulSoup(html_content, "html.parser")
            
            job_links = soup.select(self.CSS_SELECTORS["job_cards"])
            
            for link in job_links:
                href = link.get("href")
                if not href:
                    continue
                    
                full_url = self._normalize_url(href, "https://nofluffjobs.com", region_code)
                
                if '/job/' in full_url:
                    job_urls.add(full_url)
            
            self.logger.info(f"Extracted {len(job_urls)} unique job URLs from listing page")
            
            sample_size = min(5, len(job_urls))
            sample_urls = list(job_urls)[:sample_size]
            self.logger.info(f"Sample URLs: {sample_urls}")
            
        except Exception as e:
            self.logger.error(f"Error extracting job URLs: {str(e)}")
            
        return list(job_urls)
        
    async def _process_job_batches(self, job_urls: List[str], batch_size: int = 250, max_retries: int = 3):
        """
        Process job URLs in memory-efficient batches with retry logic for failed requests.
        Uses BaseScraper's batch processing with retries.
        
        :param job_urls: List of job URLs to process
        :param batch_size: Number of URLs to process in each batch
        :param max_retries: Maximum number of retries for failed requests
        """
        async def process_batch(session, batch):
            self.logger.info(f"Processing batch of {len(batch)} job URLs")
            
            detail_pages = await self._fetch_multiple_pages_async(session, batch)
            
            retry_urls = []
            
            raw_job_data = []
            timestamp = datetime.now().isoformat()
            processed = 0
            
            for j, html in enumerate(detail_pages):
                job_url = batch[j]
                processed += 1
                
                if not html:
                    self.logger.warning(f"Failed to fetch {job_url}, adding to retry queue")
                    retry_urls.append(job_url)
                    continue
                    
                try:
                    extracted_data = self._extract_raw_job_data(html, job_url)
                    
                    raw_job = {
                        "job_url": job_url,
                        "html": html,
                        "extracted_data": extracted_data,
                        "timestamp": timestamp,
                        "session_id": self.session_id,
                        "source_platform": "NoFluffJobs",
                        "type": "job_detail"
                    }
                    raw_job_data.append(raw_job)
                except Exception as e:
                    self.logger.error(f"Error processing {job_url}: {str(e)}")
                    retry_urls.append(job_url)
            
            saved = 0
            if raw_job_data:
                self._save_data(raw_job_data)
                saved = len(raw_job_data)
                self.logger.info(f"Saved {saved} raw job records to MongoDB")
                
            raw_job_data = None
            detail_pages = None
            
            return {
                'processed': processed,
                'saved': saved,
                'failed': len(retry_urls),
                'retry_urls': retry_urls
            }
        
        async with aiohttp.ClientSession() as session:
            results = await self.process_batch_with_retries(
                job_urls, process_batch, session, batch_size, max_retries
            )
            
        return results

    def _extract_raw_job_data(self, html: str, job_url: str) -> Dict[str, Any]:
        """
        Extract structured data from job detail page HTML, but preserve all raw data.
        Unlike transformation, this only extracts data without normalization.
        
        :return: Dictionary of raw job data extracted from HTML
        """
        if not html:
            return {}
            
        soup = BeautifulSoup(html, "html.parser")
        job_data = {"job_url": job_url}
        
        job_data["title"] = self._extract_text(soup, self.CSS_SELECTORS["title"])
        job_data["company"] = self._extract_text(soup, self.CSS_SELECTORS["company"])
        
        specs = {}
        for spec in soup.select(self.CSS_SELECTORS["all_specs"]):
            spec_type = None
            for attr in spec.attrs:
                if attr.startswith('data-cy') and attr != 'data-cy':
                    spec_type = attr.replace('data-cy-posting-specs-', '')
                    break
                    
            if not spec_type:
                spec_type = f"spec_{len(specs)}"
                
            specs[spec_type] = spec.get_text(strip=True)
            
        job_data["specifications"] = specs
        
        job_data["short_description"] = self._extract_text(soup, self.CSS_SELECTORS["short_description"])
        job_data["description"] = self._extract_text(soup, self.CSS_SELECTORS["description"])
        job_data["daily_tasks"] = self._extract_text(soup, self.CSS_SELECTORS["daily_tasks"])
        job_data["textual_requirements"] = self._extract_text(soup, self.CSS_SELECTORS["textual_requirements"])
        job_data["methodology"] = self._extract_text(soup, self.CSS_SELECTORS["methodology"])
        job_data["seniority_level"] = self._extract_text(soup, self.CSS_SELECTORS["seniority_level"])
        job_data["work_location"] = self._extract_text(soup, self.CSS_SELECTORS["work_location"])
        job_data["company_headquarters"] = self._extract_text(soup, self.CSS_SELECTORS["location"])
        
        job_data["requirements"] = [
            req.get_text(strip=True) 
            for req in soup.select(self.CSS_SELECTORS["requirements"] + " li")
        ] if soup.select_one(self.CSS_SELECTORS["requirements"]) else []
        
        job_data["nice_to_have"] = [
            req.get_text(strip=True) 
            for req in soup.select(self.CSS_SELECTORS["nice_to_have"] + " li")
        ] if soup.select_one(self.CSS_SELECTORS["nice_to_have"]) else []
        
        job_data["provided_equipment"] = [
            eq.get_text(strip=True) 
            for eq in soup.select(self.CSS_SELECTORS["provided_equipment"] + " li")
        ] if soup.select_one(self.CSS_SELECTORS["provided_equipment"]) else []
        
        office_benefits = soup.select_one(self.CSS_SELECTORS["office_benefits"])
        if office_benefits:
            job_data["office_benefits"] = [
                benefit.get_text(strip=True) 
                for benefit in office_benefits.select("li")
            ]
        else:
            job_data["office_benefits"] = []
            
        extra_benefits = soup.select_one(self.CSS_SELECTORS["extra_benefits"])
        if extra_benefits:
            job_data["extra_benefits"] = [
                benefit.get_text(strip=True) 
                for benefit in extra_benefits.select("li")
            ]
        else:
            job_data["extra_benefits"] = []
        
        job_data["benefits"] = [
            benefit.get_text(strip=True) 
            for benefit in soup.select(self.CSS_SELECTORS["benefits"])
        ]
        
        apply_button = soup.select_one(self.CSS_SELECTORS["apply_button"])
        if apply_button and apply_button.has_attr("href"):
            job_data["apply_url"] = apply_button["href"]
        
        sections = {}
        for section in soup.select(self.CSS_SELECTORS["all_sections"]):
            section_id = section.get('id', '')
            section_class = " ".join(section.get('class', []))
            section_key = section_id or section_class or f"section_{len(sections)}"
            sections[section_key] = section.get_text(strip=True)
            
        job_data["sections"] = sections
            
        return job_data

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


if __name__ == "__main__":
    import asyncio
    
    async def run_test():
        scraper = NoFluffJobsScraper()
        test_url = "https://nofluffjobs.com/hu/jobs"
        await scraper.scrape_async(test_url)
    
    print("Starting NoFluffJobs scraper test...")
    asyncio.run(run_test())
    print("Scraper test completed.")
