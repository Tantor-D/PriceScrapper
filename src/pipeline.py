import json
import os
import datetime
import pandas as pd
from bs4 import BeautifulSoup
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
from src.scrapers.amazon_search_spider import AmazonSearchSpider


from src.extractors.amazon_extractor import AmazonExtractor
from src.processors.post_processor import PostProcessor
from src.utils import get_unique_filename, copy_and_rename_json, delete_file

class ScraperPipeline:
    def __init__(self, site, config):
        assert site in ['Amazon'], "Only 'Amazon' is currently supported."

        self.site = site
        self.brand = config.get("Brand", "")
        self.category = config.get("Category", "")
        self.market_country = config.get("Market", "amazon.de")
        self.max_pages = config.get("Max_pages", 2)

        # search_term 自动拼接逻辑
        self.search_term = config.get("Search_term")
        if not self.search_term:
            self.search_term = f"{self.brand} {self.category}".strip()

        self.date = datetime.datetime.now().strftime("%m-%d")
        self.scraped_file = "./scraped_data.json"
        self.output_excel = get_unique_filename(f"./results/{self.site.lower()}_products_{self.date}.xlsx")

    def run_scraper(self):
        """Run Scrapy spider to collect raw HTML content from Amazon."""
        temp_scraped_data_file = "./scraped_data.json"
        output_file = get_unique_filename("./temp/scraped_data.json")
        
        print("🚀 启动 Scrapy 爬虫...")
        
        # 删除旧的临时文件以避免冲突，爬虫系统会自动创建新的文件，如果原先文件就存在的话会对内容进行追加而不是覆盖导致出错
        delete_file(temp_scraped_data_file)

        process = CrawlerProcess(get_project_settings())
        process.crawl(
            AmazonSearchSpider,
            base_url=self.market_country,
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

    def extract_data(self, scraped_pages):
        extractor = AmazonExtractor()
        all_products = []
        for page in scraped_pages:
            products = extractor.parse_products(page['html'], self.market_country)
            all_products.extend(products)
        return pd.DataFrame(all_products)

    def post_process(self, df):
        return PostProcessor().remove_duplicates(df)

    def save_to_excel(self, df):
        # 添加元信息列
        df["Date"] = self.date
        df["Market"] = self.market_country
        df["Brand"] = self.brand
        df["Category"] = self.category
        df["Search_Term"] = self.search_term

        df.to_excel(self.output_excel, index=False)
        print(f"📁 数据已保存到: {self.output_excel}")

    def run_pipeline(self):
        print(f"🔍 正在抓取 '{self.search_term}' 商品数据（市场: {self.market_country}）...")
        scraped_pages = self.run_scraper()
        if not scraped_pages:
            print("⚠️ 无有效抓取结果，流程终止。")
            return

        df = self.extract_data(scraped_pages)
        print(f"📦 提取商品总数: {len(df)}")

        df_clean = self.post_process(df)
        print(f"🧹 去重后商品数: {len(df_clean)}")

        self.save_to_excel(df_clean)
