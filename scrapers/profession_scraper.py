from typing import List, Dict, Optional
from base_scraper import BaseScraper
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor


class ProfessionScraper(BaseScraper):
    def __init__(self, scraper_name: str = "ProfessionScraper"):
        super().__init__(scraper_name)
        self.base_url = "https://www.profession.hu"

    def extract_job_urls(self, start_url: str) -> List[str]:
        """Extract all job listing URLs from the main listing page."""
        job_urls = []
        url = start_url

        while url:
            html_content = self._fetch_page(url)
            if not html_content:
                break

            soup = BeautifulSoup(html_content, "html.parser")
            job_links = soup.select("div.card-footer.bottom span.actions > a")
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

            url = self.get_next_page(html_content) 

        return job_urls

    def extract_job_details(
        self, html_content: str, job_url: str
    ) -> Dict[str, Optional[str]]:
        """Extracts job details from a job's detailed page."""
        soup = BeautifulSoup(html_content, "html.parser")
        return {
            "title": self._extract_text(soup, "h2.job-card__title a"),
            "company": self._extract_text(soup, "div.job-card__company-name a"),
            "location": self._extract_text(soup, "div.job-card__company-address span"),
            "job_url": job_url,
            "full_description": self._extract_full_description(soup),
        }

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
        """Main method to scrape job listings and details in parallel."""
        job_urls = self.extract_job_urls(start_url)

        job_html_pages = self._fetch_multiple_pages(job_urls)

        with ThreadPoolExecutor(max_workers=10) as executor:
            job_data = list(
                executor.map(self.extract_job_details, job_html_pages, job_urls)
            )

        self._save_data(job_data)
        print(f"Scraping completed. {len(job_data)} jobs saved.")


if __name__ == "__main__":
    scraper = ProfessionScraper()
    scraper.scrape("https://www.profession.hu/allasok")
