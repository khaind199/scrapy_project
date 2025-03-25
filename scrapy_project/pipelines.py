# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

from elasticsearch import Elasticsearch
# useful for handling different item types with a single interface
class ElasticsearchPipeline:
    def __init__(self, es_host, es_port, es_index):
        self.es_host = es_host
        self.es_port = es_port
        self.es_index = es_index
    
    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            es_host=crawler.settings.get('ES_HOST', 'localhost'),
            es_port=crawler.settings.get('ES_PORT', 9200),
            es_index=crawler.settings.get('ES_INDEX', 'news_articles')
        )
    
    def open_spider(self, spider):
        self.es = Elasticsearch([{'host': self.es_host, 'port': self.es_port, 'scheme': 'http'}])
        
    def process_item(self, item, spider):
        # Spider base đã xử lý lưu vào ES, nên ở đây có thể bỏ qua
        # hoặc thêm logic bổ sung nếu cần
        return item
