import aiohttp
from typing import List, Dict, Optional
from bs4 import BeautifulSoup
from scrapers.base_scraper import BaseScraper


class ProfessionScraper(BaseScraper):
    """
    A scraper class that retrieves job listings from a specified site, parses job details,
    and saves them in batches. It inherits from BaseScraper to leverage asynchronous data
    fetching, caching, and logging capabilities.
    """

    def __init__(
        self,
        scraper_name: str = "ProfessionScraper",
        proxies: Optional[List[str]] = None,
        log_dir: str = "logs",
    ):
        super().__init__(scraper_name, proxies, log_dir)
        self.base_url: str = "https://www.profession.hu"
        self.jobs_per_page: int = 20

    async def scrape_async(self, start_url: str):
        """
        Asynchronously scrape job listings from the specified start URL.

        Determines the total number of job postings, iterates through
        the calculated pages, collects job URLs, extracts detailed
        information, and saves data in batches. Progress is logged at
        each step.

        Parameters:
            start_url (str): The initial page URL to commence scraping.

        Returns:
            None
        """

        self.logger.info(f"[SCRAPE START] {start_url}")

        async with aiohttp.ClientSession() as session:
            first_page_html = await self._fetch_page_async(session, start_url)
            if not first_page_html:
                self.logger.error("[ERROR] Could not fetch the first page.")
                return

            soup = BeautifulSoup(first_page_html, "html.parser")
            job_count_div = soup.select_one("#jobs_block_count > div")
            if not job_count_div:
                self.logger.error("[ERROR] Could not find job count element.")
                return

            job_count_text = (
                job_count_div.get_text(strip=True).split(" ")[0].replace(".", "")
            )
            total_jobs = int(job_count_text)
            total_pages = (total_jobs + self.jobs_per_page - 1) // self.jobs_per_page

            self.logger.info(
                f"[INFO] Found {total_jobs} jobs, estimated {total_pages} pages."
            )

            page_urls = [
                f"{self.base_url}/allasok/{i}" for i in range(1, total_pages + 1)
            ]
            pages_html = await self._fetch_multiple_pages_async(session, page_urls)

            job_urls = []
            for html_content in pages_html:
                if html_content:
                    soup = BeautifulSoup(html_content, "html.parser")
                    job_links = soup.select(
                        "div.card-footer.bottom span.actions > a, div.job-card a"
                    )
                    for link in job_links:
                        href = link.get("href")
                        if href:
                            job_urls.append(
                                self.base_url + href if href.startswith("/") else href
                            )

            self.logger.info(
                f"[JOB URLS] Found {len(job_urls)} job links. Now fetching details..."
            )

            job_data = []
            batch_size = 1000
            batch_count = 1

            for i in range(0, len(job_urls), batch_size):
                batch = job_urls[i : i + batch_size]
                self.logger.info(
                    f"[BATCH] Fetching batch {batch_count}, size: {len(batch)}"
                )

                job_details_pages = await self._fetch_multiple_pages_async(
                    session, batch
                )

                job_data = []

                for j, html_content in enumerate(job_details_pages):
                    if html_content:
                        job_url = batch[j]
                        details = self.extract_job_details(html_content, job_url)
                        if details:
                            job_data.append(details)

                self._save_data(job_data)
                self.logger.info(
                    f"[BATCH SAVE] Saved batch {batch_count} with {len(job_data)} records."
                )
                del job_data
                batch_count += 1

            self.logger.info("[SCRAPE DONE]")

    def extract_job_details(
        self, html_content: str, job_url: str
    ) -> Dict[str, Optional[str]]:
        """
        Extracts job details from the provided HTML content using BeautifulSoup.

        Args:
            html_content (str): The raw HTML content of a job listing page.
            job_url (str): The direct URL to the job posting.

        Returns:
            Dict[str, Optional[str]]: A dictionary containing extracted job details such as
            title, company, location, job URL, and a full description.
        """

        soup = BeautifulSoup(html_content, "html.parser")

        return {
            "title": self._extract_text(soup, "#job-title"),
            "company": self._extract_text(
                soup,
                "#main > div:nth-child(1) > div > div.adv-cover-wrapper > div.adv-cover > div > div > div > section > ul > li:nth-child(1) > div > h2",
            ),
            "location": self._extract_text(
                soup,
                "#main > div:nth-child(1) > div > div.adv-cover-wrapper > div.adv-cover > div > div > div > section > ul > li:nth-child(2) > div > div.my-auto > h2",
            ),
            "job_url": job_url,
            "full_description": self._extract_text(
                soup,
                "#content > div > div:nth-child(1) > div > div > div.wrap > div > div > section",
            ),
            "source_platform": "Profession.hu",
            "session_id": self.session_id,
        }

    def _extract_text(self, soup: BeautifulSoup, selector: str) -> Optional[str]:
        """
        Extracts text from the first HTML element matching the given CSS selector.

        :param soup: A BeautifulSoup instance representing the parsed HTML.
        :param selector: A string containing a CSS selector to locate the desired element.
        :return: The stripped text of the found element, or None if the element is not found.
        """

        elem = soup.select_one(selector)
        return elem.get_text(strip=True) if elem else None


if __name__ == "__main__":
    scraper = ProfessionScraper()
    scraper.scrape("https://www.profession.hu/allasok")
