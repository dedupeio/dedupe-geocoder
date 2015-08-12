import csv
import datetime
from csvkit.cleanup import RowChecker
from dateutil.parser import parse
import sqlalchemy as sa
from collections import OrderedDict

TRUE_VALUES = ('yes', 'y', 'true', 't')
FALSE_VALUES = ('no', 'n', 'false', 'f')

DEFAULT_DATETIME = datetime.datetime(9999, 12, 31, 0, 0, 0)
NULL_DATE = datetime.date(9999, 12, 31)
NULL_TIME = datetime.time(0, 0, 0)

class TypeInferer(object):
    def __init__(self, 
                 fpath, 
                 encoding='utf-8', 
                 delimiter=',', 
                 quoting=csv.QUOTE_MINIMAL):

        self.fpath = fpath
        self.encoding = encoding
        self.delimiter = delimiter
        self.quoting = quoting
        
        with open(self.fpath, 'r', encoding=self.encoding) as f:
            reader = csv.reader(f, delimiter=self.delimiter, quoting=self.quoting)
            self.header = next(reader)
        
        self.types = OrderedDict()

    def iterColumn(self, col_idx):
        with open(self.fpath, 'r', encoding=self.encoding) as f:
            reader = csv.reader(f, delimiter=self.delimiter, quoting=self.quoting)
            header = next(reader)
            checker = RowChecker(reader)
            for row in checker.checked_rows():
                try:
                    yield row[col_idx]
                except IndexError:
                    continue

    def infer(self):
        for idx, col in enumerate(self.header):
            self.tryAll(col, idx)

    def tryAll(self, col, idx):
        try:
            self.types[col] = self.tryBoolean(idx)
            return
        except ValueError:
            pass
        
        try:
            self.types[col] = self.tryInteger(idx)
            return
        except ValueError as e:
            pass
        
        try:
            self.types[col] = self.tryFloat(idx)
            return
        except ValueError as e:
            pass
        
        try:
            self.types[col] = self.tryDateTime(idx)
            return
        except (TypeError, ValueError) as e:
            pass
        
        try:
            self.types[col] = self.tryDate(idx)
            return
        except (TypeError, ValueError) as e:
            pass

        self.types[col] = sa.String

    def tryBoolean(self, col_idx):
        for x in self.iterColumn(col_idx):
            if x.lower() in TRUE_VALUES:
                continue
            elif x.lower() in FALSE_VALUES:
                continue
            else:
                raise ValueError('Not boolean')
        return sa.Boolean
    
    def tryInteger(self, col_idx):
        for x in self.iterColumn(col_idx):
            if x == '':
                continue
            
            if isinstance(x, int):
                continue

            try:
                int_x = int(x.replace(',', ''))
            except ValueError as e:
                raise e
            
            try:
                if x[0] == '0' and int(x) != 0:
                    raise ValueError('Not integer')
            except ValueError as e:
                raise e
        
        return sa.Integer
    
    def tryFloat(self, col_idx):
        for x in self.iterColumn(col_idx):
            if x == '':
                continue
            
            try:
                float_x  = float(x.replace(',', ''))
            except ValueError as e:
                raise e

        return sa.Float

    def tryDate(self, col_idx):
        for x in self.iterColumn(col_idx):
            if x == '' or x is None:
                continue
            
            try:
                d = parse(x, default=DEFAULT_DATETIME)
            except TypeError as e:
                raise e

            # Is it only a time?
            if d.date() == NULL_DATE:
                raise ValueError('Not a Date')

            # Is it only a date?
            elif d.time() == NULL_TIME:
                continue
        
        return sa.Date

    def tryDateTime(self, col_idx):
        for x in self.iterColumn(col_idx):
            if x == '' or x is None:
                continue
            
            try:
                d = parse(x, default=DEFAULT_DATETIME)
            except TypeError as e:
                raise e

            # Is it only a time?
            if d.date() == NULL_DATE:
                raise ValueError('Not a DateTime')

            # Is it only a date?
            elif d.time() == NULL_TIME:
                raise ValueError('Not a DateTime')
            # It must be a date and time
            else:
                continue
        
        return sa.DateTime


if __name__ == "__main__":
    fpath = 'downloads/FiledDocs.txt'
    
    inferer = TypeInferer(fpath)
    inferer.infer()

    import json
    print(json.dumps(inferer.types, indent=4))

