# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from scrapy.exceptions import DropItem

import mysql.connector
from datetime import date
import re

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
        adapter = ItemAdapter(item)
        today = date.today()

        self.cursor.execute("""
            INSERT INTO daily_trending_book (date, title, author, published_date) 
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
            published_date = VALUES(published_date)
        """, (
            today,
            adapter.get('title'),
            adapter.get('author'),
            adapter.get('published_date')
        ))
        self.connection.commit()
        return item



class ValidateBookItemPipeline:

    def process_item(self, item, spider):
        adapter = ItemAdapter(item)
        published_date = adapter.get("published_date")
        published_year = re.search(r'\b(1[0-9]{3}|20[0-9]{2}|21[0-9]{2}|22[0-9]{2})\b', published_date)

        if published_year and int(published_year.group(0)) >= 2020:
            return item
        else:
            raise DropItem("Published date does not exist or too old")