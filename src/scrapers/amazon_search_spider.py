import scrapy


class AmazonSearchSpider(scrapy.Spider):
    name = "amazon_search"
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/108.0.0.0 Safari/537.36",
        "ROBOTSTXT_OBEY": False,  # Amazon's robots.txt disallows scraping; ignore it
        "DOWNLOAD_DELAY": 2,
        "FEEDS": {
            "scraped_data.json": {
                "format": "json",
                "encoding": "utf-8",
            },
        },
    }

    def __init__(self, base_url="amazon.com", search_term="", max_pages=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Ensure base_url has no protocol or path (just domain)
        self.base_url = base_url.strip().replace("http://", "").replace("https://", "").rstrip("/")
        self.search_term = search_term
        self.max_pages = int(max_pages)
        # Construct the initial search URL(s)
        # Start from page 1; Scrapy will call parse() for each request
        self.start_urls = [
            f"https://{self.base_url}/s?k={self.search_term}&page={page}"
            for page in range(1, self.max_pages + 1)
        ]

    def parse(self, response):
        """Parse a search results page and yield the HTML (or items) for extraction."""
        page_number = response.url.split("&page=")[-1] if "&page=" in response.url else "1"
        self.logger.info(f"Scraping {self.base_url} page {page_number} for '{self.search_term}'")
        html_content = response.text

        # Yield the raw HTML content for this page as an item, or store it for later processing
        yield {"page_number": page_number, "html": html_content}

        # (Alternatively, we could call an extractor here to yield structured data directly)
