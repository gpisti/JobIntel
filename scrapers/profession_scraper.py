from typing import List, Dict, Optional
import json
from scrapers.base_scraper import BaseScraper
from bs4 import BeautifulSoup

class ProfessionScraper(BaseScraper):
    def __init__(
        self,
        scraper_name: str = "ProfessionScraper",
        proxies: Optional[List[str]] = None,
        log_dir: str = "logs",
    ):
        super().__init__(scraper_name, proxies, log_dir)
        self.base_url = "https://www.profession.hu"

    def extract_job_urls(self, url: str) -> List[str]:
        """Extract job listing URLs from the main listing page."""
        job_urls = []
        while url:
            self.logger.info(f"Fetching job listings from: {url}")
            html_content = self._fetch_page(url)
            if not html_content:
                self.logger.error("Failed to fetch job listings page.")
                break

            soup = BeautifulSoup(html_content, "html.parser")
            job_links = soup.select(
                "div.card-footer.bottom span.actions > a, div.job-card a"
            )
            job_urls.extend(
                [
                    (
                        self.base_url + link["href"]
                        if link["href"].startswith("/")
                        else link["href"]
                    )
                    for link in job_links
                    if "href" in link.attrs
                ]
            )

            next_button = soup.select_one("#cvdb-list_block_pager-next")
            if next_button:
                next_href = next_button.get("href")
                url = (
                    self.base_url + next_href
                    if next_href and next_href.startswith("/")
                    else next_href
                )
            else:
                url = None

        return job_urls

    def extract_job_details(self, job_url: str) -> Dict[str, Optional[str]]:
        """Extract job details from a job's detailed page."""
        self.logger.info(f"Fetching job details from: {job_url}")
        html_content = self._fetch_page(job_url)
        if not html_content:
            self.logger.error(f"Failed to fetch job details page: {job_url}")
            return {}

        soup = BeautifulSoup(html_content, "html.parser")
        job_details = {
            "title": self._extract_text(soup, "h2.job-card__title a"),
            "company": self._extract_text(soup, "div.job-card__company-name a"),
            "location": self._extract_text(soup, "div.job-card__company-address span"),
            "job_url": job_url,
            "full_description": self._extract_full_description(soup),
        }
        return job_details

    def _extract_text(self, soup: BeautifulSoup, selector: str) -> Optional[str]:
        """Helper method to extract text from a BeautifulSoup object using a CSS selector."""
        element = soup.select_one(selector)
        return element.get_text(strip=True) if element else None

    def _extract_full_description(self, soup: BeautifulSoup) -> Optional[str]:
        """Extract the full job description from the detailed job description section."""
        description_section = soup.select_one(
            "div.job-description, div.full-job-description, div.job-details"
        )
        return description_section.get_text(strip=True) if description_section else None

    def scrape(self, start_url: str):
        """Main method to scrape job listings and details."""
        job_urls = self.extract_job_urls(start_url)
        job_data = []

        for job_url in job_urls:
            job_details = self.extract_job_details(job_url)
            if job_details:
                job_data.append(job_details)

        self._save_data(job_data)
        self.logger.info(f"Scraping completed. {len(job_data)} jobs saved.")


if __name__ == "__main__":
    scraper = ProfessionScraper()
    scraper.scrape("https://www.profession.hu/allasok")
