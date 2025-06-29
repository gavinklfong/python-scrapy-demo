# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import mysql.connector
from datetime import date

class MySQLBookPipeline:
    def open_spider(self, spider):
        self.connection = mysql.connector.connect(
            host='localhost',
            user='root',
            password='passme',
            database='demo'  
        )
        self.cursor = self.connection.cursor()
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS daily_trending_book (
                date DATE NOT NULL,
                title VARCHAR(255) NOT NULL,
                author VARCHAR(255) NOT NULL,
                published_date VARCHAR(255),
                PRIMARY KEY (date, title, author)
            )
        """)
        self.connection.commit()

    def close_spider(self, spider):
        self.cursor.close()
        self.connection.close()

    def process_item(self, item, spider):
        today = date.today()

        self.cursor.execute("""
            INSERT INTO daily_trending_book (date, title, author, published_date) VALUES (%s, %s, %s, %s)
        """, (
            today,
            item.get('title'),
            item.get('author'),
            item.get('published_date')
        ))
        self.connection.commit()
        return item
