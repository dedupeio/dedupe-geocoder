import requests
import re
import os
import sqlalchemy as sa
from typeinferer import TypeInferer

vacant_buildings = 'https://data.cityofchicago.org/api/views/7nii-7srd/rows.csv?accessType=DOWNLOAD'

class ETLThing(object):

    def __init__(self, connection):
        self.connection = connection
        self.csv_file_path = 'downloads/vacant_buildings.csv'
        self.table_name = 'vacant_buildings'
        self.primary_key = 'service_request_number'
    
    def run(self, download=False):
        
        if download:
            self.download()
        
        self.createTable()
        self.bulkInsertData()
        self.addCompleteAddress()

    def download(self):
        addresses = requests.get(vacant_buildings, stream=True)
        
        with open(self.csv_file_path, 'wb') as f:
            for chunk in addresses.iter_content(chunk_size=1024):
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

        sql_table = sa.Table(self.table_name, 
                             sa.MetaData())

        for column_name, column_type in inferer.types.items():
            column_name = self.slugify(column_name)
            col = sa.Column(column_name, column_type())
            
            sql_table.append_column(col)
            
            sql_table.append_column(sa.Column('id', 
                                              sa.Integer, 
                                              primary_key=True))
        
        dialect = sa.dialects.postgresql.dialect()
        create_table = str(sa.schema.CreateTable(sql_table)\
                           .compile(dialect=dialect)).strip(';')

        self.executeTransaction('DROP TABLE IF EXISTS {0}'.format(self.table_name))
        self.executeTransaction(create_table)

    def bulkInsertData(self):
        import psycopg2
        from geocoder.app_config import DB_USER, DB_PW, DB_HOST, \
            DB_PORT, DB_NAME
        
        DB_CONN_STR = 'host={0} dbname={1} user={2} port={3}'\
            .format(DB_HOST, DB_NAME, DB_USER, DB_PORT)

        copy_st = ''' 
            COPY {0} ({1}) FROM STDIN WITH (FORMAT CSV, DELIMITER ',', FORCE_NULL ({1}))
        '''.format(self.table_name, ','.join(self.fieldnames))
        
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
        '''.format(self.table_name)

        self.executeTransaction(address_field)

        add_complete_address = ''' 
            UPDATE vacant_buildings SET 
              complete_address = subq.complete_address
            FROM (
                SELECT 
                  service_request_number,
                  TRIM(COALESCE(LOWER(address_street_number::VARCHAR), '')) || ' ' || 
                  TRIM(COALESCE(LOWER(address_street_direction::VARCHAR), '')) || ' ' || 
                  TRIM(COALESCE(LOWER(address_street_name::VARCHAR), '')) || ' ' || 
                  TRIM(COALESCE(LOWER(address_street_suffix::VARCHAR), '')) AS complete_address
                FROM vacant_buildings
            ) AS subq
            WHERE vacant_buildings.service_request_number = subq.service_request_number
        '''

        self.executeTransaction(add_complete_address)

if __name__ == "__main__":
    import argparse
    from sqlalchemy import create_engine

    parser = argparse.ArgumentParser(
        description='Bulk load vacant building data into a PostgreSQL database.'
    )

    parser.add_argument('--download', 
                        action='store_true', 
                        help='Download fresh address data from Cook County')

    parser.add_argument('--load_data', 
                        action='store_true', 
                        help='Load address data into database')
    
    parser.add_argument('--train',
                        action='store_true',
                        help="Train an already initialized database")
    
    parser.add_argument('--block',
                        action='store_true',
                        help="Pre-block incoming data")
    
    parser.add_argument('--link',
                        action='store_true',
                        help="Link messy data")

    args = parser.parse_args()
    
    if args.load_data:
        engine = create_engine('postgresql://localhost:5432/geocoder')
        connection = engine.connect()
        
        etl = ETLThing(connection)
        etl.run(download=args.download)

        connection.close()

    if args.train:
        from geocoder.deduper import DatabaseGazetteer
        import simplejson as json
        import dedupe

        engine = create_engine('postgresql://localhost:5432/geocoder')
        
        deduper = DatabaseGazetteer([{'field': 'complete_address', 'type': 'Address'}],
                                    engine=engine)

        vacant_buildings = ''' 
            SELECT id, complete_address
            FROM vacant_buildings
        '''

        curs = engine.execute(vacant_buildings)

        messy_data = ({'complete_address': r.complete_address} for r in curs)

        deduper.drawSample(messy_data, sample_size=30000)
        
        if os.path.exists('data/training.json'):
            print('reading labeled examples from data/training.json')
            with open('data/training.json') as tf :
                deduper.readTraining(tf)
        
        dedupe.consoleLabel(deduper)

        deduper.train(ppc=0.1, index_predicates=False)
        
        # When finished, save our training away to disk
        with open('data/training.json', 'w') as tf :
            deduper.writeTraining(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open('geocoder/dedupe.settings', 'wb') as sf :
            deduper.writeSettings(sf)

        deduper.cleanupTraining()

    if args.block:
        from geocoder.deduper import StaticDatabaseGazetteer

        engine = create_engine('postgresql://localhost:5432/geocoder')
        
        with open('geocoder/dedupe.settings', 'rb') as sf:
            deduper = StaticDatabaseGazetteer(sf, engine=engine)
        
        deduper.createMatchBlocksTable(table_to_block='vacant_buildings',
                                       match_blocks_table='vb_match_blocks')

    if args.link:
        from geocoder.deduper import AddressLinkGazetteer

        engine = create_engine('postgresql://localhost:5432/geocoder')
        
        with open('geocoder/dedupe.settings', 'rb') as sf:
            deduper = AddressLinkGazetteer(sf, engine=engine)
        
        messy_data_info = {
            'messy_data_table': 'vacant_buildings',
            'messy_blocks_table': 'vb_match_blocks'
        }

        matches = deduper.match(messy_data_info, n_matches=5, threshold=0.75)
        
        # From here, take the matches and update the messy data source with 
        # canonical IDs. Or something ...
        print(len(matches))
