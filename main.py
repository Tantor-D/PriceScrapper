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


def run_scraper(search_term, base_url, max_pages):
    """Runs the Scrapy crawler and fetches raw HTML for given search term."""
    # Define the output file
    output_file = "scraped_data.json"

    # # Ensure previous output file is removed
    # if os.path.exists(output_file):
    #     os.remove(output_file)
    #
    # # Run the Scrapy Spider
    # process = CrawlerProcess(get_project_settings())
    # process.crawl(AmazonSearchSpider, base_url=base_url, search_term=search_term, max_pages=max_pages)
    # process.start()
    #
    # # Check if file was generated
    # if not os.path.exists(output_file):
    #     print("Scraping failed or no results found.")
    #     return []

    # Read scraped JSON output
    with open(output_file, "r", encoding="utf-8") as f:
        scraped_data = json.load(f)

    data = scraped_data
    for i, page in enumerate(data[:2]):  # 只检查前几页，避免数据太多
        html_content = page["html"]
        soup = BeautifulSoup(html_content, "html.parser")

        # 查找是否包含商品列表
        product_cards = soup.select('div[data-component-type="s-search-result"]')

        if product_cards:
            print(f"✅ 第 {i + 1} 页包含 {len(product_cards)} 个商品")
        else:
            print(f"❌ 第 {i + 1} 页未找到任何商品，可能被 Amazon 反爬")


    return scraped_data

def main():
    # Set parameters
    search_term = "baby bottles"   # Change to your desired search query
    base_url = "amazon.com"  # Use amazon.de for Germany
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
    output_csv = "amazon_products.csv"
    df_unique.to_csv(output_csv, index=False)
    print(f"Scraped data saved to {output_csv}")

if __name__ == "__main__":
    main()
