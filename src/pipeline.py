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

from src.processors.post_processor import PostProcessor
from src.utils import get_unique_filename, copy_and_rename_json, delete_file, get_market_country_based_on_url

allowed_retailer_urls = ["amazon.de", "meds.se"]

class ScraperPipeline:
    def __init__(self, retailer_url, config):
        assert retailer_url in allowed_retailer_urls, f"not supported retailer_url: {retailer_url}, allowed: {allowed_retailer_urls}"
        
        # 一经确认不能修改的参数，主要是爬虫的基本信息
        self.retailer_url = retailer_url
        self.market_country = get_market_country_based_on_url(retailer_url)
        self.search_spider, self.extractor = self.get_spider_and_extractor(retailer_url)
        
        
        # 自动更新的信息，日期和输出文件名，不会因为每次的检索需求变数而变化
        self.date = datetime.datetime.now().strftime("%m-%d")
        
        
        # 需要外界输入的参数，用于决定每次的爬虫行为
        self.brand = config.get("Brand", "")
        self.category = config.get("Category", "")
        self.max_pages = config.get("Max_pages", 2)
        self.output_excel = get_unique_filename(f"./results/{self.retailer_url.replace(".", "-").lower()}_{self.date}_Brand-{self.brand}_Category-{self.category}.xlsx")        
        self.search_term = config.get("Search_term", f"{self.brand} {self.category}".strip())


    def run_scraper(self, debug=False):
        """Run Scrapy spider to collect raw HTML content from Amazon."""
        temp_scraped_data_file = "./scraped_data.json"
        output_file = get_unique_filename("./temp/scraped_data.json")
        
        if debug:
            with open(temp_scraped_data_file, "r", encoding="utf-8") as f:
                scraped_data = json.load(f)
            # 基础分析 HTML 中商品存在情况
            cards = []
            for i, page in enumerate(scraped_data[:2]):
                html = page["html"]
                soup = BeautifulSoup(html, "html.parser")
                products = soup.select('div[data-component-type="s-search-result"]')
                if products:
                    print(f"✅ 第 {i + 1} 页包含 {len(products)} 个商品")
                    cards.extend(products)
                else:
                    print(f"❌ 第 {i + 1} 页未找到任何商品，可能被反爬")
            return scraped_data
        
        print("🚀 启动 Scrapy 爬虫...")
        
        # 删除旧的临时文件以避免冲突，爬虫系统会自动创建新的文件，如果原先文件就存在的话会对内容进行追加而不是覆盖导致出错
        delete_file(temp_scraped_data_file)

        process = CrawlerProcess(get_project_settings())
        process.crawl(
            self.search_spider,
            base_url=self.retailer_url,
            search_term=self.search_term,
            max_pages=self.max_pages
        )
        process.start()

        if not os.path.exists(temp_scraped_data_file):
            print("❌ Scraping 失败或未找到结果。")
            return []

        # 复制并重命名 JSON 文件，确保唯一性，如果未来需要回溯数据可以直接使用这个文件
        copy_and_rename_json(temp_scraped_data_file, output_file)
        
        # 加载 JSON 数据，完成爬虫检索
        with open(output_file, "r", encoding="utf-8") as f:
            scraped_data = json.load(f)

        return scraped_data

    def extract_data(self, scraped_pages):
        all_products = []
        for page in scraped_pages:
            products = self.extractor.parse_products(page['html'])
            all_products.extend(products)
        return pd.DataFrame(all_products)

    def post_process(self, df):
        return PostProcessor().remove_duplicates(df)

    def save_to_excel(self, df):
        # 添加元信息列
        df["Date"] = self.date
        df["Market"] = self.market_country
        df["Retail"] = self.retailer_url        
        df["Brand"] = self.brand
        df["Category"] = self.category
        df["Search Keywords"] = self.search_term

        df.to_excel(self.output_excel, index=False)
        print(f"📁 数据已保存到: {self.output_excel}")

    def run_pipeline(self):
        print(f"🔍 正在抓取 '{self.search_term}' 商品数据（市场: {self.retailer_url}）...")
        scraped_pages = self.run_scraper()
        if not scraped_pages:
            print("⚠️ 无有效抓取结果，流程终止。")
            return

        df = self.extract_data(scraped_pages)
        print(f"📦 提取商品总数: {len(df)}")

        df_clean = self.post_process(df)
        print(f"🧹 去重后商品数: {len(df_clean)}")
        
        self.save_to_excel(df_clean)

    def get_spider_and_extractor(self, retailer_url):
        if retailer_url == "amazon.de":
            return AmazonSearchSpider, AmazonExtractor
        elif retailer_url == "meds.se":
            return MedsSearchSpider, MedsExtractor()
        else:
            raise ValueError(f"Unsupported retailer URL: {retailer_url}")