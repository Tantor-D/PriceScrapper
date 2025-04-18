import pandas as pd
from src.scrapers.amazon_search_spider import AmazonSearchSpider
from src.processors.post_processor import PostProcessor
from src.extractors.amazon_extractor import AmazonExtractor

from bs4 import BeautifulSoup
import json
import os
import scrapy
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings
import shutil

def get_unique_filename(file_path):
    # 创建文件所在的目录（如果不存在）
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    # 尝试找到一个不存在的文件名，防止覆盖原文件
    if not os.path.exists(file_path):
        return file_path  # 文件不存在，直接返回原路径

    base, ext = os.path.splitext(file_path)
    counter = 1
    new_path = f"{base}_{counter}{ext}"

    while os.path.exists(new_path):
        counter += 1
        new_path = f"{base}_{counter}{ext}"

    return new_path


def copy_and_rename_json(src_path, dest_path):
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"源文件不存在: {src_path}")

    # 确保目标路径唯一
    unique_dest = get_unique_filename(dest_path)

    # 复制文件到目标路径
    shutil.copy2(src_path, unique_dest)
    print(f"文件已复制并重命名为: {unique_dest}")


def run_scraper(search_term, base_url, max_pages):
    """Runs the Scrapy crawler and fetches raw HTML for given search term."""
    # Define the output file
    temp_scraped_data_file = "./scraped_data.json"
    output_file = get_unique_filename("./temp/scraped_data.json")
    output_file = "./scraped_data.json"
    # Run the Scrapy Spider
    process = CrawlerProcess(get_project_settings())
    process.crawl(AmazonSearchSpider, base_url=base_url, search_term=search_term, max_pages=max_pages)
    process.start()
    
    # Check if file was generated
    if not os.path.exists(temp_scraped_data_file):
        print("Scraping failed or no results found.")
        return []
    else:
        # Move the temp file to the final output file
        copy_and_rename_json(temp_scraped_data_file, output_file)
    
    # Read scraped JSON output
    with open(output_file, "r", encoding="utf-8") as f:
        scraped_data = json.load(f)


    # do a basic data analyse
    data = scraped_data
    cards = []
    for i, page in enumerate(data[:2]):  # 只检查前几页，避免数据太多
        html_content = page["html"]
        soup = BeautifulSoup(html_content, "html.parser")

        # 查找是否包含商品列表
        product_cards = soup.select('div[data-component-type="s-search-result"]')

        if product_cards:
            print(f"✅ 第 {i + 1} 页包含 {len(product_cards)} 个商品")
            cards.extend(product_cards)  # 直接添加到cards列表中，方便后续处理
        else:
            print(f"❌ 第 {i + 1} 页未找到任何商品，可能被 Amazon 反爬")
        
    return scraped_data

def main():
    # Set parameters
    search_term = "baby bottles"   # Change to your desired search query
    base_url = "amazon.de"  # Use amazon.de for Germany
    max_pages = 2            # Adjust as needed

    # Step 1: Scrape Amazon search pages
    print(f"Scraping Amazon ({base_url}) for '{search_term}' up to {max_pages} pages...")
    scraped_pages = run_scraper(search_term, base_url, max_pages)
    if not scraped_pages:
        print("No data scraped. Exiting.")
        return

    # Step 2: Extract product data
    extractor = AmazonExtractor()
    all_products = []
    for page in scraped_pages:
        products = extractor.parse_products(page['html'], base_url)
        all_products.extend(products)

    # Convert extracted data to DataFrame
    df = pd.DataFrame(all_products)
    print(f"Total extracted products: {len(df)}")

    # Step 3: Post-process data (remove duplicates)
    processor = PostProcessor()
    df_unique = processor.remove_duplicates(df)

    print(f"Products after deduplication: {len(df_unique)}")

    # Step 4: Save the cleaned data
    # output_csv = "amazon_products.csv"
    # df_unique.to_csv(output_csv, index=False)
    # print(f"Scraped data saved to {output_csv}")
    output_excel = "amazon_products.xlsx"
    df_unique.to_excel(output_excel, index=False)
    print(f"Scraped data saved to {output_excel}")



from src.pipeline import ScraperPipeline
def main1():
    config = {
        "Category": "Pacifier Box",
        "Brand": "BIBS",
        # "Search_term": "BIBS Pacifier Box",
        # 没有 Search_term，默认会使用 BIBS Pacifier
    }
    meds_pipeline = ScraperPipeline(retailer_url="meds.se", config=config)
    meds_pipeline.run_pipeline()

    # 这个现在是不支持的
    # apotea_pipeline = ScraperPipeline(retailer_url="apotea.se", config=config)
    # apotea_pipeline.run_pipeline()
    
    # amazon.de 的爬虫配置
    # pipeline = ScraperPipeline(retailer_url="amazon.de", config=config)
    # pipeline.run_pipeline()



if __name__ == "__main__":
    # main()
    
    main1()
    
    # with open("./scraped_data.json", "r", encoding='utf-8') as f:
    #     aa = json.load(f)
    #     print(aa)
