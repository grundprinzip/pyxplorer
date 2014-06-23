__author__ = 'grund'
import re

from snakebite.client import Client
import pyhs2


class Loader:
    """
    The idea of the loader is to provide a convenient interface to create a new table
    based on some input files
    """

    def __init__(self, path, name_node, hive_server,
                 user="root", hive_db="default", password=None, nn_port=8020, hive_port=10000):

        # HDFS Connection
        self._client = Client(name_node, nn_port)

        self._db = hive_db

        # Hive Connection
        self._hive = pyhs2.connect(host=hive_server,
                                   port=hive_port,
                                   authMechanism="PLAIN",
                                   database=hive_db,
                                   user=user,
                                   password=password)
        self._path = path


    def load(self):
        # Check data to see which kind it is
        files = self._client.ls([self._path])

        files = [f for f in files if f['file_type'] == 'f']
        if len(files) == 0:
            raise Exception("Cannot load empty directory")

        # Pick the first file and assume that it has the same content as the others
        data = self.head(files[0]['path'])
        res = self.check_separator(data)
        if res == None:
            # We cant load the data and better abort here
            print("cant load data, cannot find a separator")
            return

        sep = res[0]
        num_cols = res[1]

        # Build table statement
        table_statement, table_name = self._create_table(self._path, sep, num_cols)
        cursor = self._hive.cursor()
        cursor.execute(table_statement)

        return self._db, table_name


    def _create_table(self, path, sep, count):
        buf = """CREATE EXTERNAL TABLE pyxplorer_data (
    %s
    )ROW FORMAT DELIMITED FIELDS TERMINATED BY '%s'
    STORED AS TEXTFILE LOCATION '%s'
    """ % (",".join(["col_%d string" % x for x in range(count)]), sep, path)
        return buf, "pyxplorer_data"

    def check_separator(self, data):
        """
        THis method evaluates a list of separators on the input data to check which one
        is correct. This is done by first splitting the input by newline and then
        checking if the split by separator is equal for each input row except the last
        that might be incomplete due to the limited input data

        :param data: input data to check
        :return:
        """

        sep_list = [r'\t', r';', r',', r'|', r'\s+']

        data_copy = data
        for sep in sep_list:
            # Check if the count matches each line
            splitted = data_copy.split("\n")
            parts = [len(re.split(sep, line)) for line in splitted]

            # If we did not split anything continue
            if sum(parts) == len(splitted):
                continue

            diff = 0

            for i in range(len(parts[1:-1])):
                diff += abs(parts[i] - parts[i + 1])

            if diff == 0:
                return sep, parts[0]

        # If we reach this point we did not find a separator
        return None


    def head(self, file_path):
        """
        Onlye read the first packets that come, try to max out at 1024kb

        :return: up to 1024b of the first block of the file
        """
        processor = lambda path, node, tail_only=True, append=False: self._handle_head(
            path, node)

        # Find items and go
        for item in self._client._find_items([file_path], processor,
                                             include_toplevel=True,
                                             include_children=False, recurse=False):
            if item:
                return item

    def _handle_head(self, path, node, upper=1024 * 1024):
        data = ''
        for load in self._client._read_file(path, node, tail_only=False,
                                            check_crc=False):
            data += load
            if (len(data) > upper):
                return data

        return data
