# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


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
