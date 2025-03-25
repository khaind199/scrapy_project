# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# useful for handling different item types with a single interface

from elasticsearch import Elasticsearch
from datetime import datetime
import logging

class ElasticsearchPipeline:
    def __init__(self, es_host, es_port, es_index):
        self.es_host = es_host
        self.es_port = es_port
        self.es_index = es_index
        self.logger = logging.getLogger(__name__)

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            es_host=crawler.settings.get('ES_HOST', 'localhost'),
            es_port=crawler.settings.get('ES_PORT', 9200),
            es_index=crawler.settings.get('ES_INDEX', 'news_articles')
        )
    
    def open_spider(self, spider):
        """Kết nối Elasticsearch khi spider bắt đầu"""
        self.es = Elasticsearch(
            [{'host': self.es_host, 'port': self.es_port, 'scheme': 'http'}],
            verify_certs=False
        )
        
        # Tạo index nếu chưa tồn tại
        if not self.es.indices.exists(index=self.es_index):
            self._create_index()

    def _create_index(self):
       
        mapping = {
            "mappings": {
                "properties": {
                    "title": {"type": "text", "analyzer": "vi_analyzer"},
                    "link": {"type": "keyword"},
                    "description": {"type": "text"},
                    "content": {"type": "text", "analyzer": "vi_analyzer"},
                    "publish_time": {"type": "date"},
                    "crawl_time": {"type": "date"},
                    "source": {"type": "keyword"}
                }
            },
            "settings": {
                "analysis": {
                    "analyzer": {
                        "vi_analyzer": {
                            "tokenizer": "standard",
                            "filter": ["lowercase", "ascii_folding"]
                        }
                    }
                }
            }
        }
        
        self.es.indices.create(index=self.es_index, body=mapping)

    def process_item(self, item, spider):
        """Xử lý item và lưu vào Elasticsearch"""
        try:
            doc = {
                'title': item.get('title', ''),
                'link': item.get('link', ''),
                'description': item.get('description', ''),
                'content': item.get('content', ''),
                'publish_time': item.get('publish_time'),
                'crawl_time': item.get('crawl_time'),
                'source': item.get('source', '')
            }
            
            # Index document với ID là URL (tránh trùng lặp)
            self.es.index(
                index=self.es_index,
                id=item['link'],
                document=doc
            )
            
            self.logger.info(f"Đã lưu bài viết: {item['title'][:50]}...")
            
        except Exception as e:
            self.logger.error(f"Lỗi khi lưu vào Elasticsearch: {str(e)}", exc_info=True)
        
        return item

    def close_spider(self, spider):
        """Đóng kết nối khi spider kết thúc"""
        if hasattr(self, 'es'):
            self.es.transport.close()