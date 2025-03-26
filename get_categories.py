import requests
from bs4 import BeautifulSoup
import pika
import json

# Cấu hình RabbitMQ
RABBITMQ_HOST = 'localhost'
RABBITMQ_QUEUE = 'crawl_tasks'

def get_vnexpress_categories():
    """Lấy danh sách categories từ trang chủ VnExpress"""
    url = "https://vnexpress.net/"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        categories = []
        
        # Lấy các danh mục chính từ menu
        menu = soup.find('ul', class_='parent')
        if menu:
            for item in menu.find_all('li'):
                link = item.find('a')
                if link and link.get('href'):
                    categories.append(link['href'])
        
        return categories
    except Exception as e:
        print(f"Lỗi khi lấy categories: {str(e)}")
        return []

def send_to_rabbitmq(categories):
    """Gửi danh sách categories vào RabbitMQ"""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
    
    for category in categories:
        message = {
            'url': category,
            'source': 'vnexpress',
            'priority': 1
        }
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Làm message persistent
            )
        )
        print(f"Đã gửi: {category}")
    
    connection.close()

if __name__ == '__main__':
    categories = get_vnexpress_categories()
    if categories:
        send_to_rabbitmq(categories)
        print(f"Đã gửi {len(categories)} danh mục vào RabbitMQ")
    else:
        print("Không lấy được danh mục từ VnExpress")