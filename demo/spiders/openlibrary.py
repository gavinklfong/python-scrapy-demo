import scrapy

from ..items import BookItem

def extract_with_css(item, query):
    return item.css(query).get(default="").strip()

class OpenLibrarySpider(scrapy.Spider):
    name = "openlibrary"

    start_urls = ["https://openlibrary.org/trending/daily"]

    def start_requests(self):        
        for url in self.start_urls:
            yield scrapy.Request(url=url, callback=self.parse_book_list)
            

    def parse_book_list(self, response):
        book_items = response.css("#contentBody > div.mybooks-list > ul > li > div > div.details")
        if not book_items:
            self.logger.warning("No book items found on the page.")
            return
        
        # Extract book details
        for item in book_items:
            book = BookItem()
            book['title'] = item.css('h3.booktitle > a::text').get(default='').strip()
            book['author'] = extract_with_css(item, "span.bookauthor > a::text")
            book['published_date'] = extract_with_css(item, "span.resultStats > span.resultDetails > span:nth-child(1)::text")
            yield book

        # Handle pagination
        # next_page = response.css("#contentBody > div.pager > div > a:last-child::attr(href)").get()
        # if next_page:
        #     yield response.follow(next_page, self.parse_book_list)
