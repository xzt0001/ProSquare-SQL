import json
import os
from collections import defaultdict

class IndexManager:
    def __init__(self, index_dir="data/indexes"):
        self.index_dir = index_dir
        os.makedirs(self.index_dir, exist_ok=True)
        self.indexes = {}  # In-memory index cache

    def get_index_file(self, table_name, column_name):
        """ Returns the file path for a given table/column index. """
        return os.path.join(self.index_dir, f"{table_name}_{column_name}.json")

    def load_index(self, table_name, column_name):
        """ Loads the index from disk into memory. """
        index_file = self.get_index_file(table_name, column_name)
        if os.path.exists(index_file):
            with open(index_file, "r") as f:
                self.indexes[(table_name, column_name)] = json.load(f)
        else:
            self.indexes[(table_name, column_name)] = {}

    def save_index(self, table_name, column_name):
        """ Saves the in-memory index to disk. """
        index_file = self.get_index_file(table_name, column_name)
        with open(index_file, "w") as f:
            json.dump(self.indexes[(table_name, column_name)], f, indent=4)

    def create_index(self, table_name, column_name, table_data):
        """ Builds an index for a specific column. """
        self.indexes[(table_name, column_name)] = defaultdict(list)
        for row in table_data:
            if column_name in row:
                self.indexes[(table_name, column_name)][row[column_name]].append(row["id"])
        self.save_index(table_name, column_name)

    def lookup(self, table_name, column_name, value):
        """ Returns a list of matching row IDs for a given value. """
        self.load_index(table_name, column_name)
        return self.indexes.get((table_name, column_name), {}).get(value, [])

    def update_index(self, table_name, column_name, old_value, new_value, row_id):
        """ Updates the index when a value is changed. """
        self.load_index(table_name, column_name)
        if old_value in self.indexes[(table_name, column_name)]:
            self.indexes[(table_name, column_name)][old_value].remove(row_id)
            if not self.indexes[(table_name, column_name)][old_value]:  
                del self.indexes[(table_name, column_name)][old_value]
        self.indexes[(table_name, column_name)][new_value].append(row_id)
        self.save_index(table_name, column_name)

    def delete_from_index(self, table_name, column_name, value, row_id):
        """ Removes a value from the index when a row is deleted. """
        self.load_index(table_name, column_name)
        if value in self.indexes[(table_name, column_name)]:
            self.indexes[(table_name, column_name)][value].remove(row_id)
            if not self.indexes[(table_name, column_name)][value]:  
                del self.indexes[(table_name, column_name)][value]
        self.save_index(table_name, column_name)

