import json
import pika
from scrapy import Spider
from scrapy.http import Request
from elasticsearch import Elasticsearch
from urllib.parse import urljoin

class BaseSpider(Spider):
    """
    Base spider with RabbitMQ and Elasticsearch integration
    """
    
    # Cấu hình Elasticsearch
    es_host = "localhost"
    es_port = 9200
    es_index = "news_articles"
    
    # Cấu hình RabbitMQ
    rabbitmq_host = "localhost"
    rabbitmq_queue = "crawl_tasks"
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
        # Kết nối Elasticsearch
        self.es = Elasticsearch([{'host': self.es_host, 'port': self.es_port, 'scheme': 'http'}])
        
        # Kết nối RabbitMQ
        self.connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=self.rabbitmq_host))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue=self.rabbitmq_queue)
    
    def get_start_urls_from_rabbitmq(self):
        """Lấy danh sách URL cần crawl từ RabbitMQ"""
        urls = []
        
        def callback(ch, method, properties, body):
            task = json.loads(body)
            urls.append(task['url'])
            ch.basic_ack(delivery_tag=method.delivery_tag)
        
        # Lấy tất cả message trong queue
        while True:
            method_frame, _, body = self.channel.basic_get(self.rabbitmq_queue)
            if not method_frame:
                break
            callback(self.channel, method_frame, None, body)
        
        return urls
    
    def save_to_elasticsearch(self, item):
        """Lưu dữ liệu vào Elasticsearch"""
        doc = {
            'title': item.get('title', ''),
            'link': item.get('link', ''),
            'description': item.get('description', ''),
            'content': item.get('content', ''),
            'publish_time': item.get('publish_time', ''),
            'source': self.name
        }
        
        # Tạo index nếu chưa tồn tại
        if not self.es.indices.exists(index=self.es_index):
            self.es.indices.create(index=self.es_index)
        
        # Index document
        self.es.index(index=self.es_index, document=doc)
    
    def closed(self, reason):
        """Đóng kết nối khi spider kết thúc"""
        self.connection.close()
    
    def make_absolute_url(self, base_url, relative_url):
        """Chuyển URL tương đối thành tuyệt đối"""
        return urljoin(base_url, relative_url)