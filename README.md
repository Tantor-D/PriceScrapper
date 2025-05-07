# PriceScrapper

## Overview

PriceScrapper is a Python-based web scraping tool designed to extract product data from multiple e-commerce platforms such as Amazon, Meds, and Apotea. It provides a modular architecture to scrape, process, and extract data, saving the results in Excel format for further analysis.

The project is built with a clear pipeline structure, where the main entry point is the `ScraperPipeline`. The pipeline orchestrates the scraping, processing, and extraction of data in a seamless workflow.

## Description

A pipeline consists of three parts: the scraper (responsible for scraping and saving a JSON file), the extractor (extracts information from the JSON, such as the number of reviews, title, URL, etc.), and the processors (post-processing; currently, it only includes deduplication).

You only need to focus on:

* `main.py`: The entry point for the entire workflow
* `pipeline.py`: Just take a quick look to understand how it calls the scraper and extractors
* `scraper.py`: This part was generated with the help of ChatGPT, and it mainly involves calling libraries
* `amazon_extractor.py`: A customized information extractor for the Amazon website
