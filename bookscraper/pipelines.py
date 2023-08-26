# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html

# using environmental variables
from dotenv import load_dotenv
import os

load_dotenv()  # Load variables from .env file

# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

# processing the data (data transformation, validations, storage into databases)
class BookscraperPipeline:
    def process_item(self, item, spider):
        adapter = ItemAdapter(item)

        ## Strip all whitespaces from strings
        field_names = adapter.field_names()
        for field_name in field_names:
            value = adapter.get(field_name)
            adapter[field_name] = value.strip()
        
        ## Category & Product Type --> switch to lowercase
        lowercase_keys = ['category', 'product_type']
        for lowercase_key in lowercase_keys:
            value = adapter.get(lowercase_key)
            adapter[lowercase_key] = value.lower()

        ## Availability --> extract number of books in stock
        availability_string = adapter.get('availability')
        split_string_array = availability_string.split('(')
        if len(split_string_array) < 2:
            adapter['availability'] = 0
        else:
            availability_array = split_string_array[1].split(' ')
            adapter['availability'] = int(availability_array[0])

        ## Number Of Reviews --> convert to int
        reviews_string = adapter.get('num_reviews')
        adapter['num_reviews'] = int(reviews_string)

        ## Price --> convert to float
        price_keys = ['price', 'price_excl_tax', 'price_incl_tax', 'tax']
        for price_key in price_keys:
            value = adapter.get(price_key)
            value = value.replace('£', '')
            adapter[price_key] = float(value)

        ## Star Rating --> convert text to number
        rating_string = adapter.get('star_rating')
        split_rating_array = rating_string.split(' ')
        rating_text_value = split_rating_array[1].lower()
        if rating_text_value == "zero":
            adapter['star_rating'] = 0
        elif rating_text_value == "one":
            adapter['star_rating'] = 1
        elif rating_text_value == "two":
            adapter['star_rating'] = 2
        elif rating_text_value == "three":
            adapter['star_rating'] = 3
        elif rating_text_value == "four":
            adapter['star_rating'] = 4
        elif rating_text_value == "five":
            adapter['star_rating'] = 5
        
        return item

## pipeline for saving sata inton the mysql database
import mysql.connector

class SaveToMySQLPipeline:
    def __init__(self):
        self.conn = mysql.connector.connect(
            host = 'localhost',
            user = 'root',
            password = os.getenv("DATABASE_PASSWORD"),
            database = 'books_data'
        )

        ## create cursor used to execute commands
        self.cur = self.conn.cursor()

        ## Create books table if none exists
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS books(
            id int NOT NULL auto_increment, 
            url VARCHAR(255),
            title text,
            product_type VARCHAR(255),
            price_excl_tax DECIMAL,
            price_incl_tax DECIMAL,
            tax DECIMAL,
            price DECIMAL,
            availability INTEGER,
            num_reviews INTEGER,
            star_rating INTEGER,
            category VARCHAR(255),
            PRIMARY KEY (id)
        )
        """)

    def process_item(self, item, spider):

        ## Define insert statement
        self.cur.execute(""" insert into books (
            url, 
            title,
            product_type, 
            price_excl_tax,
            price_incl_tax,
            tax,
            price,
            availability,
            num_reviews,
            star_rating,
            category
            ) values (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
                )""", (
            item["url"],
            item["title"],
            item["product_type"],
            item["price_excl_tax"],
            item["price_incl_tax"],
            item["tax"],
            item["price"],
            item["availability"],
            item["num_reviews"],
            item["star_rating"],
            item["category"]
        ))

        # ## Execute insert of data into database
        self.conn.commit()
        return item

    
    def close_spider(self, spider):

        ## Close cursor & connection to database 
        self.cur.close()
        self.conn.close()


