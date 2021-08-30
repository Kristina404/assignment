import abc
import csv
from enum import Enum, auto
from typing import Tuple, Optional, Dict, Iterator, Any


class Operation(Enum):
    CREATE = auto()
    UPDATE = auto()
    DELETE = auto()


# Note the methods of this ProductStreamProcessor class should not be adjusted
# as this is a hypothetical base class shared with other programs.
class ProductStreamProcessor(metaclass=abc.ABCMeta):
    # a function to initiate 'before' and 'after' files
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
    def match_data(self, data_list, match_key, match_value):
        list_of_data_matches = list(filter(
            lambda data_list_item: data_list_item[match_key] == match_value, data_list))
        if len(list_of_data_matches) > 0:
            return list_of_data_matches[0]
        else:
            return None

    def map_data(self, reader_list, headers_list):
        list_of_data = []
        for row in reader_list:
            dictionary_of_data_before = {'data': {}}
            for index in range(len(row)):
                column_name = headers_list[index]
                dictionary_of_data_before['id'] = row[0]
                dictionary_of_data_before['data'][column_name] = row[index]
            list_of_data.append(dictionary_of_data_before)
        return list_of_data

    def main(self):
        with open(self.path_to_before_csv) as path_to_before_csv, open(self.path_to_after_csv) as path_to_after_csv:
            reader_before = csv.reader(path_to_before_csv)
            reader_after = csv.reader(path_to_after_csv)
            next(reader_before)
            headers = next(reader_after)

            list_of_data_before = self.map_data(reader_before, headers)
            list_of_data_after = self.map_data(reader_after, headers)

            for dictionary_in_list_of_data_before in list_of_data_before:
                id_match = self.match_data(
                    list_of_data_after, 'id', dictionary_in_list_of_data_before['id'])
                if id_match is not None:
                    update = Operation.UPDATE
                    yield update, dictionary_in_list_of_data_before['id'], id_match['data']
                else:
                    delete = Operation.DELETE
                    yield delete, dictionary_in_list_of_data_before['id'], None

            for dictionary_in_list_of_data_after in list_of_data_after:
                id_match = self.match_data(
                    list_of_data_before, 'id', dictionary_in_list_of_data_after['id'])
                if id_match is None:
                    create = Operation.CREATE
                    yield create, dictionary_in_list_of_data_after['id'], dictionary_in_list_of_data_after['data']
