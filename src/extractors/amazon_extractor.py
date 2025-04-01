

from parsel import Selector
from urllib.parse import urljoin
from typing import List, Dict, Optional


class AmazonExtractor:
    def parse_products(self, html_content: str, 
                       base_url: str = "https://www.amazon.com") -> List[Dict[str, Optional[str]]]:
        """解析 Amazon 搜索结果页面的 HTML，提取产品信息"""
        sel = Selector(text=html_content)
        products = []
        base_http_url = base_url if base_url.startswith("http") else "https://" + base_url

        for card in sel.css('div[data-component-type="s-search-result"]'):
            product = self.extract_product_info(card, base_http_url)
            if product:
                products.append(product)

        import json
        with open('../../products.json', 'w') as f:
            json.dump(products, f, indent=4)  # 保存到json文件中
        return products

    def extract_product_info(self, card, base_http_url: str) -> Optional[Dict[str, Optional[str]]]:
        """从单个产品卡片提取信息"""
        title = self.extract_title(card)
        if not title:
            return None

        price, currency = self.extract_price_and_currency(card)
        rating = self.extract_rating(card)
        reviews_count = self.extract_reviews_count(card)
        link = self.extract_link(card, base_http_url)
        pack_size = self.extract_pack_size(card)

        return {
            "Title": title,
            "Price": price,
            "Currency": currency,
            "Rating": rating,
            "ReviewsCount": reviews_count,
            "PackSize": pack_size,
            "Link": link
        }

    def extract_title(self, card) -> Optional[str]:
        """提取产品标题"""
        title = card.css('h2 a span::text').get() or card.css('h2 a::attr(aria-label)').get() or card.css(
            'img::attr(alt)').get()
        return title.strip() if title else None

    def extract_price_and_currency(self, card) -> (Optional[str], Optional[str]):
        """提取价格和货币种类"""
        price_whole = card.css('span.a-price-whole::text').get()
        price_fraction = card.css('span.a-price-fraction::text').get()
        currency_symbol = card.css('span.a-price-symbol::text').get() or card.css('span.a-offscreen::text').re_first(
            r'^[^\d]+')

        if price_whole and price_fraction:
            price = f"{price_whole.strip()}.{price_fraction.strip()}"
        else:
            price = card.css('span.a-offscreen::text').get()

        if price:
            price = price.strip().replace("$", "").replace(",", "").replace("€", "").replace("£", "")

        currency = currency_symbol.strip() if currency_symbol else None
        return price, currency

    def extract_rating(self, card) -> Optional[str]:
        """提取评分"""
        rating = card.css('span.a-icon-alt::text').get()
        return rating.split()[0] if rating else None

    def extract_reviews_count(self, card) -> Optional[str]:
        """提取评论数量"""
        reviews_count = card.css('span.a-size-base.s-underline-text::text').get()
        return reviews_count.strip().replace(",", "") if reviews_count else None

    def extract_link(self, card, base_http_url: str) -> Optional[str]:
        """提取产品链接"""
        rel_link = card.css('a::attr(href)').get()
        return urljoin(base_http_url, rel_link) if rel_link else None

    def extract_pack_size(self, card) -> Optional[str]:
        """提取包装规格"""
        pack_size = card.css('div.a-row.a-size-base span.a-size-base::text').get()
        return pack_size.strip() if pack_size else None
