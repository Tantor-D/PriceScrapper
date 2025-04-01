from parsel import Selector
from urllib.parse import urljoin
from typing import List, Dict, Optional
import json


class MedsExtractor:
    def parse_products(self, html_content: str,
                       base_url: str = "https://www.meds.se") -> List[Dict[str, Optional[str]]]:
        """解析 meds.se 搜索结果页面 HTML，提取产品信息"""
        sel = Selector(text=html_content)
        products = []

        for card in sel.css('div.product-card'):
            product = self.extract_product_info(card, base_url)
            if product:
                products.append(product)

        # 可选：保存为本地 JSON 文件
        with open('../../meds_products.json', 'w', encoding='utf-8') as f:
            json.dump(products, f, indent=4, ensure_ascii=False)

        return products

    def extract_product_info(self, card, base_url: str) -> Optional[Dict[str, Optional[str]]]:
        """从单个产品卡片中提取信息"""
        title = self.extract_title(card)
        if not title:
            return None

        price, currency = self.extract_price_and_currency(card)
        link = self.extract_link(card, base_url)
        image = self.extract_image(card, base_url)

        return {
            "Title": title,
            "Price": price,
            "Currency": currency,
            "Link": link,
            "Image": image
        }

    def extract_title(self, card) -> Optional[str]:
        """提取产品标题"""
        title = card.css('span.display-name::text').get()
        return title.strip() if title else None

    def extract_price_and_currency(self, card) -> (Optional[str], Optional[str]):
        """提取价格与货币"""
        price_text = card.css('div.displayed-price::text').get()
        if price_text:
            price = price_text.strip().replace("kr", "").replace(",", ".").strip()
            return price, "SEK"
        return None, None

    def extract_link(self, card, base_url: str) -> Optional[str]:
        """提取产品详情页链接"""
        rel_link = card.css('a::attr(href)').get()
        return urljoin(base_url, rel_link) if rel_link else None

    def extract_image(self, card, base_url: str) -> Optional[str]:
        """提取产品图片链接"""
        image_url = card.css('img::attr(src)').get()
        return urljoin(base_url, image_url) if image_url else None
