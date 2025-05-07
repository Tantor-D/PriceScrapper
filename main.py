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
    # Create the directory if it does not exist
    directory = os.path.dirname(file_path)
    if directory and not os.path.exists(directory):
        os.makedirs(directory)
    
    # Try to find a non-existing filename to avoid overwriting the original file
    if not os.path.exists(file_path):
        return file_path  # File does not exist, return original path

    base, ext = os.path.splitext(file_path)
    counter = 1
    new_path = f"{base}_{counter}{ext}"

    while os.path.exists(new_path):
        counter += 1
        new_path = f"{base}_{counter}{ext}"

    return new_path


def copy_and_rename_json(src_path, dest_path):
    if not os.path.exists(src_path):
        raise FileNotFoundError(f"Source file does not exist: {src_path}")

    # Ensure destination path is unique
    unique_dest = get_unique_filename(dest_path)

    # Copy file to destination path
    shutil.copy2(src_path, unique_dest)
    print(f"File has been copied and renamed to: {unique_dest}")


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

    # Do a basic data analysis
    data = scraped_data
    cards = []
    for i, page in enumerate(data[:2]):  # Only check the first few pages to avoid too much data
        html_content = page["html"]
        soup = BeautifulSoup(html_content, "html.parser")

        # Check if it contains product listings
        product_cards = soup.select('div[data-component-type="s-search-result"]')

        if product_cards:
            print(f"✅ Page {i + 1} contains {len(product_cards)} products")
            cards.extend(product_cards)  # Add directly to cards list for later processing
        else:
            print(f"❌ Page {i + 1} found no products, possibly blocked by Amazon anti-scraping measures")
        
    return scraped_data

def main():
    # Set parameters
    search_term = "baby bottles"   # Change to your desired search query
    base_url = "amazon.de"  # Use amazon.de for Germany
    max_pages = 2           # Adjust as needed

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
        # Without Search_term, it will default to BIBS Pacifier
    }
    
    # meds.se spider call
    # meds_pipeline = ScraperPipeline(retailer_url="meds.se", config=config)
    # meds_pipeline.run_pipeline()

    # amazon.de spider call
    amazon_pipeline = ScraperPipeline(retailer_url="amazon.de", config=config)
    amazon_pipeline.run_pipeline()

if __name__ == "__main__":
    # main()
    main1()
    