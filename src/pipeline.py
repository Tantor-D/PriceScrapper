import json
import os
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from src.scrapers.amazon_search_spider import AmazonSearchSpider
from src.extractors.amazon_extractor import AmazonExtractor

from src.scrapers.meds_spider import MedsSearchSpider
from src.extractors.meds_extractor import MedsExtractor

from src.scrapers.apotea_search_spider import ApoteaSearchSpider
from src.extractors.apotea_extractor import ApoteaExtractor

from src.processors.post_processor import PostProcessor
from src.utils import get_unique_filename, copy_and_rename_json, delete_file, get_market_country_based_on_url

allowed_retailer_urls = ["amazon.de", "meds.se", "apotea.se"]

class ScraperPipeline:
    def __init__(self, retailer_url, config):
        assert retailer_url in allowed_retailer_urls, f"not supported retailer_url: {retailer_url}, allowed: {allowed_retailer_urls}"
        
        # Immutable parameters, mainly basic spider information
        self.retailer_url = retailer_url
        self.market_country = get_market_country_based_on_url(retailer_url)
        self.search_spider, self.extractor = self.get_spider_and_extractor(retailer_url)
        
        # Automatically updated information, date and output file name, not affected by each scrape config
        self.date = datetime.datetime.now().strftime("%d/%m/%Y")
        
        # Parameters provided externally, used to determine spider behavior each run
        self.brand = config.get("Brand", "")
        self.category = config.get("Category", "")
        self.max_pages = config.get("Max_pages", 2)
        self.output_excel = get_unique_filename(f"./results/{self.retailer_url.replace('.', '-').lower()}_{self.date.replace('/', '-')}_Brand-{self.brand}_Category-{self.category}.xlsx")        
        self.search_term = config.get("Search_term", f"{self.brand} {self.category}".strip())


    def run_scraper(self, debug=False):
        """Run Scrapy spider to collect raw HTML content."""
        temp_scraped_data_file = "./scraped_data.json"
        output_file = get_unique_filename("./temp/scraped_data.json")
        
        # Used only for debug; no spider run is needed, just read previous results
        if debug:
            with open(temp_scraped_data_file, "r", encoding="utf-8") as f:
                scraped_data = json.load(f)
            # Basic analysis of product existence in HTML
            cards = []
            for i, page in enumerate(scraped_data[:2]):
                html = page["html"]
                soup = BeautifulSoup(html, "html.parser")
                products = soup.select('div[data-component-type="s-search-result"]')
                if products:
                    print(f"✅ Page {i + 1} contains {len(products)} products")
                    cards.extend(products)
                else:
                    print(f"❌ Page {i + 1} found no products, possibly blocked by anti-scraping measures")
            return scraped_data
        
        print("🚀 Starting Scrapy spider...")
        
        # Delete old temp file to avoid conflicts; the spider will automatically create a new file, otherwise appends to existing content causing errors
        delete_file(temp_scraped_data_file)

        # Run spider
        process = CrawlerProcess(get_project_settings())
        process.crawl(
            self.search_spider,
            base_url=self.retailer_url,
            search_term=self.search_term,
            max_pages=self.max_pages
        )
        process.start()

        # Check if spider succeeded by checking file existence
        if not os.path.exists(temp_scraped_data_file):
            print("❌ Scraping failed or no results found.")
            return []

        # Copy and rename JSON file to ensure uniqueness; useful for future data tracing
        copy_and_rename_json(temp_scraped_data_file, output_file)
        
        # Load JSON data and complete scraping retrieval
        with open(output_file, "r", encoding="utf-8") as f:
            scraped_data = json.load(f)
        return scraped_data

    def extract_data(self, scraped_pages):
        all_products = []
        for page in scraped_pages:
            products = self.extractor.parse_products(page['html'], base_url=self.retailer_url if self.retailer_url.startswith("http") else "https://www." + self.retailer_url)
            all_products.extend(products)
        return pd.DataFrame(all_products)

    def post_process(self, df):
        return PostProcessor().remove_duplicates(df)

    def save_to_excel(self, df):
        # Add metadata columns
        df["Date"] = self.date
        df["Market"] = self.market_country
        df["Retail"] = self.retailer_url        
        df["Brand"] = self.brand
        df["Category"] = self.category
        df["Search Keywords"] = self.search_term

        df.to_excel(self.output_excel, index=False)
        print(f"📁 Data saved to: {self.output_excel}")

    def run_pipeline(self):
        print(f"🔍 Scraping '{self.search_term}' product data (market: {self.retailer_url})...")
        scraped_pages = self.run_scraper()
        if not scraped_pages:
            print("⚠️ No valid scraping results, terminating process.")
            return

        df = self.extract_data(scraped_pages)
        print(f"📦 Total products extracted: {len(df)}")

        df_clean = self.post_process(df)
        print(f"🧹 Products after removing duplicates: {len(df_clean)}")
        
        self.save_to_excel(df_clean)

    def get_spider_and_extractor(self, retailer_url):
        if retailer_url == "amazon.de":
            return AmazonSearchSpider, AmazonExtractor()
        elif retailer_url == "meds.se":
            return MedsSearchSpider, MedsExtractor()
        elif retailer_url == "apotea.se":
            return ApoteaSearchSpider, ApoteaExtractor()
        else:
            raise ValueError(f"Unsupported retailer URL: {retailer_url}")
