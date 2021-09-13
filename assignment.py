"""
Hello Python developer

Good luck with this coding exercise for becoming a Python developer at Channable!

# First, some context:
Channable is an online tool that imports products from its users' eCommerceplatforms (e.g. WooCommerce) every day,
processes those products, and sends updates for those products to marketing channels (e.g. Amazon or eBay).
Technically speaking, Channable sends ​create​, ​update, and ​delete​ operations for these products. Every day when
Channable imports the products from the eCommerce platform, Channable needs to decide which operation it needs
to send to the marketing channel:

  - Create: ​the product wasn’t imported from the eCommerce system yesterday,
    but it was imported today. This means we have to send a ​create operation
    ​to the eCommerce platform

  - Update:​ the product was imported yesterday and is also imported today,
    however, one of the values for the products has changed (e.g. the price of
    the product). This means we have to send an ​update operation​ to the
    marketing channel

  - Delete: ​the product was imported yesterday, but was not imported today.
    This means we have to send a ​delete operation ​to the marketing channel


# The assignment:
In this assignment you are asked to make a basic implementation of the logic described above. You should have
received two CSV files to resemble the data that is imported from the eCommerce system:

  - product_inventory_before.csv​ (resembles the product data that was imported yesterday)
  - product_inventory_after.csv ​(resembles the product data that was imported today)

For this assignment you need to build a program that compares the product data between the `before CSV` and
the `after CSV`. The `​id​` column can be assumed to be a unique identifier for the products in both CSVs. The
output should give the create, update, and delete operations that should be sent to the marketing channel.


# Requirements:
  - The program should be a single ​.py ​​file (no compressed files, such as .zip, .rar, etc.)
  - The program should be written in ​python 3.7​, using only python’s ​built-in libraries.
  - You have to implement the `ProductDiffer` class below and specifically its entry point called `main`.
  - The `ProductStreamProcessor` should not be changed.
  - The output of main is a sequence of operations in the form of triples that contain:
        1. the operation type
        2. the product id
        3. either a dictionary with the complete product data where the keys are the column names
           from the CSV files or a `None`


# Note:
The assignment is consciously kept a bit basic to make sure you don’t have to spend hours and hours on this
assignment. However, even though the assignment itself is quite basic, we would like you to show us how you
would structure your code to make it easily readable, so others can trust it works as intended.
"""
import abc
import csv
from enum import Enum, auto
from typing import Tuple, Optional, Dict, Iterator, Any


class Operation(Enum):
    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()


class ProductStreamProcessor(metaclass=abc.ABCMeta):
    # Note the methods of this ProductStreamProcessor class should not be adjusted
    # as this is a hypothetical base class shared with other programs.

    def __init__(self, path_to_before_csv: str, path_to_after_csv: str):
        self.path_to_before_csv = path_to_before_csv
        self.path_to_after_csv = path_to_after_csv

    @abc.abstractmethod
    def main(self) -> Iterator[Tuple[Operation, str, Optional[Dict[str, Any]]]]:
        """
        Creates a stream of operations based for products in the form of tuples
        where the first element is the operation, the second element is the id
        for the product, and the third is a dictionary with all data for a
        product. The latter is None for DELETE operations.
        """
        ...

class ProductDiffer(ProductStreamProcessor):
    def match_product(self, products, match_id):
        """
        this function finds id matches in passed data list

        Args:
            products (list): list of dictionaries with data
            match_id (str): id to be filtered by

        Returns:
            dict | None: dictionary - if there is a match or None - if there are no matches
        """
        for matched_product in filter(
            lambda product: product['id'] == match_id, products):
                return matched_product
        return None

    def convert_csv_data(self, products_from_csv, headers_list):
        """
        this function maps list with csv file data into a list of dictionaries
    
        Args:
            products_from_csv (_reader): csv reader list object iterator
            headers_list (list): list with all headers from the csv file

        Returns:
            list: list with mapped data from csv file as dictionaries
        """
        # create an empty list to collect data from csv list
        products = []
        # loop through csv rows list
        for csv_product_row in products_from_csv:
            # create an empty data dictionary to fill
            product = {'data': {}}
            product['id'] = csv_product_row[0]
            # because headers list and product list have the same lenght we can use one indexes list to iterate over both of them
            row_indexes_list = range(len(csv_product_row))
            # loop through each item in data row list and in the headers list 
            for index in row_indexes_list:
                column_name = headers_list[index]
                product['data'][column_name] = csv_product_row[index]
            products.append(product)
        # returning a list with dictionaries of data for each row in the file
        return products

    def main(self):
        """this function opens 'before CSV' and 'after CSV' files, compares data in them and yields the result operation in a tuple.  

        Returns:
            tuple : contains the operation type, the product id, either a dictionary with the complete product data where the keys are the column names from the CSV files or a `None`
        """
        # open 2 files
        with open(self.path_to_before_csv) as path_to_before_csv, open(self.path_to_after_csv) as path_to_after_csv:
            reader_before = csv.reader(path_to_before_csv)
            reader_after = csv.reader(path_to_after_csv)
            # skip header from the first file
            next(reader_before)
            # collect header from the second file for future use
            headers = next(reader_after)
            # convert csv rows of each file into lists of dictionaries
            products_before = self.convert_csv_data(reader_before, headers)
            products_after = self.convert_csv_data(reader_after, headers)
            # iterate over a list of data from 'before CSV'
            for product in products_before:
                # find id matches in 'after CSV' and 'before CSV'
                matched_product = self.match_product(
                    products_after, product['id'])
                if matched_product is not None:   
                    # if id was found in both csv files - product is updated
                    yield Operation.UPDATE, product['id'], matched_product['data']
                else:  
                    # if id is in 'before CSV' but not in 'after CSV' - product is deleted
                    yield Operation.DELETE, product['id'], None

            # iterate over a list of data from 'after CSV'
            for product in products_after:
                # find id matches in 'before CSV' and 'after CSV'
                matched_product = self.match_product(
                    products_before, product['id'])
                if matched_product is None:   
                    # if id is in 'after CSV' but not in 'before CSV' - product is created
                    yield Operation.CREATE, product['id'], product['data']
