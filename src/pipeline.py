import datetime



class ScriperPipeline:
    def __init__(self, site, config):
        assert site in ['Amazon']
        
        # 进行与site有关的检查
        
        
        search_term = "baby bottles"   # Change to your desired search query
        base_url = "amazon.de"  # Use amazon.de for Germany
        max_pages = 2            # Adjust as needed
        
        
        # info about one retrieve operation, which
        self.brand = config.get("Brand")
        self.search_term = config.get("Search_term")
        
        # info should be automatically updated with the search info
        # 之后这部分的内容要放到一个函数中，然后pipeline允许更新需要检索的关键字什么的，这里的内容就自动更新
        self.market_country = config.get("Market")
        self.retailer = base_url
        self.category = self.search_term
        self.date = datetime.datetime.now().strftime("%m-%d")  # Format: MM-DD

    def process_item(self, item, spider):
        # Do something with the item
        return