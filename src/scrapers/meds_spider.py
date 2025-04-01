import scrapy


class MedsSearchSpider(scrapy.Spider):
    name = "meds_search"
    custom_settings = {
        "USER_AGENT": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/108.0.0.0 Safari/537.36",
        "ROBOTSTXT_OBEY": False,
        "DOWNLOAD_DELAY": 2,
        "FEEDS": {
            "scraped_data.json": {
                "format": "json",
                "encoding": "utf-8",
            },
        },
    }

    def __init__(self, search_term="", max_pages=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_term = search_term
        self.max_pages = int(max_pages)

        # meds.se uses "+" for spaces in search term
        formatted_search = search_term.replace(" ", "+")
        self.start_urls = [
            f"https://www.meds.se/sok?q={formatted_search}&page={page}"
            for page in range(1, self.max_pages + 1)
        ]

    def parse(self, response):
        page_number = response.url.split("&page=")[-1] if "&page=" in response.url else "1"
        self.logger.info(f"Scraping meds.se page {page_number} for '{self.search_term}'")

        # Save raw HTML for now (could be replaced with structured extraction)
        yield {
            "page_number": page_number,
            "url": response.url,
            "html": response.text,
        }

        # 可以在这里加提取逻辑，例如解析商品信息、价格、链接等
