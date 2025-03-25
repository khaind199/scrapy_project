# from scrapy_project.spiders.base_spider import BaseSpider
# from scrapy import Request


import scrapy
from urllib.parse import urljoin
from datetime import datetime

class VnExpressSpider(scrapy.Spider):
    name = "vnexpress"
    allowed_domains = ["vnexpress.net"]
    start_urls = ["https://vnexpress.net/"]

    def parse(self, response):
        articles = response.css("article.item-news")
        
        for article in articles:
            title = article.css("h3.title-news a::text").get()
            relative_link = article.css("h3.title-news a::attr(href)").get()
            description = article.css("p.description a::text").get()
            
            if relative_link:
                link = urljoin(response.url, relative_link)
                yield response.follow(
                    link, 
                    callback=self.parse_detail, 
                    meta={
                        'title': title, 
                        'description': description
                    }
                )

    def parse_detail(self, response):
        title = response.meta['title']
        description = response.meta['description']
        
        # Lấy nội dung chính
        content = " ".join([
            p.strip() for p in 
            response.css("article.fck_detail p.Normal::text").getall()
            if p.strip()
        ])
        
        # Lấy thời gian xuất bản
        publish_time = response.css("span.date::text").get()
        
        # Chuẩn hóa thời gian
        if publish_time:
            try:
                publish_time = datetime.strptime(publish_time, '%d/%m/%Y, %H:%M').isoformat()
            except ValueError:
                publish_time = None
        
        yield {
            "title": title,
            "link": response.url,
            "description": description,
            "content": content,
            "publish_time": publish_time,
            "source": "vnexpress",
            "crawl_time": datetime.now().isoformat()
        }