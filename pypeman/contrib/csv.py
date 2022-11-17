import csv
import io

from pypeman import nodes


class CSV2Python(nodes.BaseNode):
    """
    Convert CSV File to Python
    'headers' indicate if the csv file contains a first line with column titles
    'to_dict' indicates if module use csv.DictReader or csv.reader (
        to use csv.DictReader, headers must be set to True too)
    """
    def __init__(self, *args, filepath=None, to_dict=False, headers=False,
                 delimiter=',', quoting=None, encoding=None, **kwargs):
        self.delimiter = delimiter
        self.filepath = filepath
        self.headers = headers
        self.to_dict = to_dict and headers
        self.quoting = quoting or csv.QUOTE_NONE
        self.encoding = encoding or "utf-8"
        super().__init__(*args, **kwargs)

    def process(self, msg):
        data_to_return = []
        if self.filepath:
            fpath = nodes.callable_or_value(self.filepath, msg)
        else:
            fpath = msg.meta["filepath"]
        with open(fpath, "r", encoding=self.encoding) as fin:
            if self.to_dict:
                csvdata = csv.DictReader(fin, delimiter=self.delimiter, quoting=self.quoting)
            else:
                csvdata = csv.reader(fin, delimiter=self.delimiter, quoting=self.quoting)
                if self.headers:
                    csvdata.pop(0)
            for row in csvdata:
                data_to_return.append(row)
        msg.payload = data_to_return
        return msg


class CSVstr2Python(nodes.BaseNode):
    """
    Like CSV2Python but Convert CSV string to Python
    'headers' indicate if the csv file contains a first line with column titles
    'to_dict' indicates if module use csv.DictReader or csv.reader (
        to use csv.DictReader, headers must be set to True too)
    """
    def __init__(self, *args, filepath=None, to_dict=False, headers=False,
                 delimiter=',', quoting=None, encoding=None, **kwargs):
        self.delimiter = delimiter
        self.headers = headers
        self.to_dict = to_dict and headers
        self.quoting = quoting or csv.QUOTE_NONE
        self.encoding = encoding or "utf-8"
        super().__init__(*args, **kwargs)

    def process(self, msg):
        data_to_return = []
        csvfile = io.StringIO(msg.payload)
        if self.to_dict:
            csvdata = csv.DictReader(csvfile, delimiter=self.delimiter, quoting=self.quoting)
        else:
            csvdata = csv.reader(csvfile, delimiter=self.delimiter, quoting=self.quoting)
            if self.headers:
                csvdata.pop(0)
        for row in csvdata:
            data_to_return.append(row)
        msg.payload = data_to_return
        return msg


class Python2CSVstr(nodes.BaseNode):
    """
    Convert python list of dict (like json) to csv str
    """
    def __init__(self, *args, header=False, fieldnames=None,
                 delimiter=',', quoting=None, newline="", **kwargs):
        self.header = header
        self.delimiter = delimiter
        self.fieldnames = fieldnames
        self.quoting = quoting or csv.QUOTE_NONE
        self.newline = newline
        super().__init__(*args, **kwargs)

    def process(self, msg):
        data = msg.payload
        fieldnames = self.fieldnames or list(data[0].keys())
        output = io.StringIO(newline=self.newline)
        writer = csv.DictWriter(output, fieldnames, delimiter=self.delimiter, quoting=self.quoting)
        if self.header:
            writer.writeheader()
        for line in data:
            writer.writerow(line)
        output.seek(0)
        msg.payload = output.read()
        return msg
