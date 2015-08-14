import re
import requests
from typeinferer import TypeInferer
import sqlalchemy as sa

class ETLThing(object):

    def __init__(self, 
                 connection, 
                 name, 
                 primary_key=None, 
                 address_fields=[]):

        self.connection = connection
        self.name = name
        self.name_slug = self.slugify(name)
        self.csv_file_path = 'downloads/%s.csv' % self.name_slug

        self.primary_key = None

        if primary_key:
            self.primary_key = self.slugify(primary_key)
        
        self.address_fields = [self.slugify(f) for f in address_fields]
    
    def run(self, download_url=None, messy=False):
        
        if download_url:
            self.download(download_url)
        
        self.createTable()
        self.bulkInsertData()

        if messy:
            self.addCompleteAddress()

    def download(self, download_url):
        content = requests.get(download_url, stream=True)
        
        with open(self.csv_file_path, 'wb') as f:
            for chunk in content.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
        
    def slugify(self, text, delim='_'):
        if text:
            punct_re = re.compile(r'[\t !"#$%&\'()*\-/<=>?@\[\\\]^_`{|},.:;]+')
            result = []
            for word in punct_re.split(text.lower()):
                if word:
                    result.append(str(word))
            return delim.join(result)
        else: # pragma: no cover
            return text
    
    def executeTransaction(self, query, raise_exc=False, *args, **kwargs):
        trans = self.connection.begin()

        try:
            if kwargs:
                self.connection.execute(query, **kwargs)
            else:
                self.connection.execute(query, *args)
            trans.commit()
        except sa.exc.ProgrammingError as e:
            trans.rollback()
            if raise_exc:
                raise e

    def createTable(self):
        inferer = TypeInferer(self.csv_file_path)
        inferer.infer()
        
        self.fieldnames = [self.slugify(f) for f in inferer.header]
        
        if self.primary_key and self.primary_key not in self.fieldnames:
            raise ValueError('primary key %s given is not amongst the columns (%s)' \
                                 % (self.primary_key, self.fieldnames))

        sql_table = sa.Table(self.name_slug, sa.MetaData())
        
        if not self.primary_key:
            sql_table.append_column(sa.Column('id', 
                                              sa.Integer, 
                                              primary_key=True))
            self.primary_key = 'id'
        else:
            sql_table.append_column(sa.Column(self.primary_key, 
                                              inferer.types[self.primary_key],
                                              primary_key=True))

        for column_name, column_type in inferer.types.items():
            column_name = self.slugify(column_name)
            col = sa.Column(column_name, column_type())
            
            sql_table.append_column(col)
        
        dialect = sa.dialects.postgresql.dialect()
        create_table = str(sa.schema.CreateTable(sql_table)\
                           .compile(dialect=dialect)).strip(';')

        self.executeTransaction('DROP TABLE IF EXISTS {0}'.format(self.name_slug))
        self.executeTransaction(create_table)

    def bulkInsertData(self):
        import psycopg2
        from geocoder.app_config import DB_USER, DB_PW, DB_HOST, \
            DB_PORT, DB_NAME
        
        DB_CONN_STR = 'host={0} dbname={1} user={2} port={3}'\
            .format(DB_HOST, DB_NAME, DB_USER, DB_PORT)

        copy_st = ''' 
            COPY {0} ({1}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', FORCE_NULL ({1}))
        '''.format(self.name_slug, ','.join(self.fieldnames))
        
        with open(self.csv_file_path, 'r') as f:
            next(f)
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as curs:
                    try:
                        curs.copy_expert(copy_st, f)
                    except psycopg2.IntegrityError as e:
                        print(e)
                        conn.rollback()
        
        # os.remove(self.csv_file_path)
    
    def addCompleteAddress(self):
        address_field = ''' 
            ALTER TABLE {0} ADD COLUMN complete_address VARCHAR
        '''.format(self.name_slug)

        self.executeTransaction(address_field)
        
        clauses = []

        for field in self.address_fields:
            clauses.append("TRIM(COALESCE(LOWER(%s::VARCHAR), ''))" % field)
        
        clauses = " || ' ' || ".join(clauses)

        add_complete_address = ''' 
            UPDATE {0} SET 
              complete_address = subq.complete_address
            FROM (
                SELECT 
                  {1},
                  {2} AS complete_address
                FROM {0}
            ) AS subq
            WHERE {0}.{1} = subq.{1}
        '''.format(self.name_slug, self.primary_key, clauses)

        self.executeTransaction(add_complete_address, raise_exc=True)

