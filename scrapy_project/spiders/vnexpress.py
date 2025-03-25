from scrapy_project.spiders.base_spider import BaseSpider
from scrapy import Request

class VnExpressSpider(BaseSpider):
    name = "vnexpress"
    allowed_domains = ["vnexpress.net"]
    
    def start_requests(self):
        # Lấy danh sách URL cần crawl từ RabbitMQ
        start_urls = self.get_start_urls_from_rabbitmq()
        if not start_urls:
            start_urls = ["https://vnexpress.net/"]
            
        for url in start_urls:
            yield Request(url, callback=self.parse)
    
    def parse(self, response):
        articles = response.css("article.item-news")
        
        for article in articles:
            title = article.css("h3.title-news a::text").get()
            relative_link = article.css("h3.title-news a::attr(href)").get()
            description = article.css("p.description a::text").get()
            
            if relative_link:
                link = self.make_absolute_url(response.url, relative_link)
                yield Request(
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
        content = " ".join(response.css("article.fck_detail p::text").getall()).strip()
        
        # Lấy thời gian xuất bản
        publish_time = response.css("span.date::text").get()
        
        item = {
            "title": title,
            "link": response.url,
            "description": description,
            "content": content,
            "publish_time": publish_time
        }
        
        # Lưu vào Elasticsearch
        self.save_to_elasticsearch(item)
        
        yield item