"""
We implement here CSVFile class.

After reading the data once (in O(n) time), it allows retrieving any line in O(1) time.

To initialize an object of that class, a file stream should be passed, formatted as CSV.

The class should has the following functions:
- get_line: Returns the requested line as a dict
- get_iter: Returns an iterator starting at the input line

It assumes that the the first line of the file contains the header for the values.
"""

import sys
from io import StringIO, IOBase
from typing import Dict, Iterator
import random

CHUNK_SIZE = 1000
SEPARATOR = ','


class CSVFile(object):
    def __init__(self, file: IOBase):
        self.chunk_size = CHUNK_SIZE
        line_num = 1
        """
        lines_locations[i] stores the file-offset in bytes of line i for every i such that
        i-1 is a multiple of CHUNK_SIZE.
        For example, if CHUNK_SIZE == 1000, then the keys in lines_locations dictionary
        are 1, 1001, 2001, etc.
        """
        self.lines_locations = {}
        while file.readline():
            """
            We iterate over the file and store in the map the locations doing
            steps of size CHUNK_SIZE.
            """
            location = file.tell()
            if not (line_num - 1) % self.chunk_size:
                self.lines_locations[line_num] = location
            line_num += 1
        self.file = file
        self.file.seek(0)
        self.header = file.readline()
        self.iter_line = 1
        self.length = line_num - 2
        return None

    def get_file_length(self):
        return self.length

    def get_header(self):
        return self.header

    def get_line(self, line_number: int) -> Dict:
        """Get a specific line in the CSV file
        Args:
            line_number: The line number (starting at 1, notice that 0 is the header row)
        Returns:
            A dictionary in which the keys are the column header (from the first row)
            and the values are the fields in the specific line.
        """
        start_of_chunk_line = int(line_number / self.chunk_size) * self.chunk_size + 1
        self.file.seek(self.lines_locations[start_of_chunk_line])
        offset = int(line_number % self.chunk_size)
        """
        The start_of_chunk_line is the key corresponding to line_number.
        That is, the biggest key k in the map such that k <= line_number.
        """
        counter = 1
        while counter < offset:
            """
            Once we found the corresponding chunk, we move in the file 
            doing steps of one line, until we get to the one we searched for. 
            """
            self.file.readline()
            counter += 1
        """
        Having the right line, we can build a dictionary with the line and the header, and return it.
        """
        return get_dictionary(self.header, self.file.readline())

    def __iter__(self):
        return self

    def __next__(self):
        self.iter_line += 1
        if self.iter_line <= self.length:
            return self.get_line(self.iter_line)
        raise StopIteration

    def get_iter(self, line_number: int) -> Iterator[Dict]:
        """
        Returns an iterator starting at line_number in which every iteration returns the next line
        Args:
            line_number: The line number (strating at 1, notice that 0 is the titles row)
        Returns:
            A python iterator
        """
        self.iter_line = line_number - 1
        return iter(self)


def get_line_iterating(file, line_num):
    """
    Just for testing.
    Gets line_num from file while iterating through the entire file if needed.
    :param file: A csv file.
    :param line_num: A number of line.
    :return: The requested line.
    """
    counter = 1
    while counter < line_num:
        file.readline()
        counter += 1
    return file.readline()


def get_dictionary(header, line):
    """
    Returns a dictionary corresponding to the provided header and line of a csv.
    :param header: The header of a csv.
    :param line: A line of a csv.
    :return: The corresponding dictionary.
    """
    header_list = [elem.strip() for elem in header.split(SEPARATOR)]
    line_list = [elem.strip() for elem in line.split(SEPARATOR)]
    return dict(zip(header_list, line_list))


def main():
    example()
    if len(sys.argv) == 1:
        return
    with open(sys.argv[1], 'r') as f:
        csv_file = CSVFile(f)
        random_line_num = random.randint(0, csv_file.get_file_length())
        # Just for testing
        line_iterating = get_dictionary(csv_file.get_header(), get_line_iterating(f, random_line_num))
        line_using_map = csv_file.get_line(random_line_num)
        assert line_iterating == line_using_map


def example():
    """
    Simple example of what the implemented class should do
    """
    example_csv = """age,name,color
                     23, Dan, blue
                     33, Danny, purple
                     50, Danna, red
                     22, Barbra, grey
                     55, Moshik, white"""

    csv = StringIO(example_csv)
    csv_file = CSVFile(csv)
    assert csv_file.get_line(3) == {'age': '50', 'name': 'Danna', 'color': 'red'}

    for line in csv_file.get_iter(1):
        print(line)
        # Should print every line from the first one to the last as a dictionary.


if __name__ == "__main__":
    main()
