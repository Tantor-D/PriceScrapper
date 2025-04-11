import scrapy


class ApoteaSearchSpider(scrapy.Spider):
    name = "apotea_search"
    custom_settings = {
        "DOWNLOAD_DELAY": 1.5,
        "PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT": 30 * 1000,
        "FEEDS": {
            "scraped_data_apotea.json": {
                "format": "json",
                "encoding": "utf-8",
            },
        },
        "DOWNLOAD_HANDLERS": {
            "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
            "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
        },
        "TWISTED_REACTOR": "twisted.internet.asyncioreactor.AsyncioSelectorReactor",
    }

    def __init__(self, search_term="", max_pages=1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.search_term = search_term
        self.max_pages = int(max_pages)
        formatted = search_term.replace(" ", "+")
        self.start_urls = [
            f"https://www.apotea.se/sok?q={formatted}&page={page}"
            for page in range(1, self.max_pages + 1)
        ]

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(
                url,
                meta={"playwright": True, "playwright_include_page": True},
                callback=self.parse,
            )

    async def parse(self, response):
        # 可以在此等待特定内容加载
        page = response.meta["playwright_page"]
        await page.wait_for_timeout(3000)  # 等待3秒
        await page.close()

        yield {
            "url": response.url,
            "html": response.text,
        }
