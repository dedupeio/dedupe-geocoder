import requests
import zipfile
from simpledbf import Dbf5
import re
import os
import sqlalchemy as sa

cook_county_data_portal = 'https://datacatalog.cookcountyil.gov/api/geospatial/%s?method=export&format=Original'

four_by_fours = {
    'chicago': 'jev2-4wjs', 
    'suburbs': '6mf5-x8ic'
}


class ETLThing(object):

    def __init__(self, connection):
        self.connection = connection
        self.zip_file_path = 'downloads/%s_addresses.zip' % self.region_name
        self.csv_file_path = 'downloads/%s_addresses.csv' % self.region_name
    
    def run(self, download=False):
        
        if download:
            self.download()
        
        self.createTable()
        self.bulkInsertData()

    def download(self):
        url = cook_county_data_portal % four_by_fours[self.region_name]
        
        addresses = requests.get(url, stream=True)
        
        with open(self.zip_file_path, 'wb') as f:
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
        dbf_file_path = ''

        with zipfile.ZipFile(self.zip_file_path, 'r') as zf:
            for name in zf.namelist():
                if name.endswith('.dbf'):
                    dbf_file_path = zf.extract(name, path='downloads')
        
        type_lookup = {
            'N': 'INTEGER',
            'D': 'DATE',
        }
        
        dbf = Dbf5(dbf_file_path)
        all_fields = []
        self.fieldnames = []

        for field in dbf.fields:
            name, type, length = field
            
            name = self.slugify(name)
            
            if name != 'deletionflag':
                if type == 'C':
                    sql = '%s VARCHAR(%s)' % (name, length)
                elif type == 'N' and length > 10:
                    sql = '%s DOUBLE PRECISION' % (name)
                else:
                    sql = '%s %s' % (name, type_lookup[type])

                all_fields.append(sql)
                self.fieldnames.append(name)

        fields_sql = ','.join(all_fields)
        
        create_table_sql = ''' 
            CREATE TABLE {0} ({1})
        '''.format(self.table_name, fields_sql)
        
        self.executeTransaction(create_table_sql)
        
        if os.path.exists(self.csv_file_path):
            os.remove(self.csv_file_path)

        dbf.to_csv(self.csv_file_path, header=False, chunksize=1024)

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
                        logger.error(e, exc_info=True)
                        print(e)
                        conn.rollback()
        
        # os.remove(self.csv_file_path)
    
    def mergeTables(self):
        final_fields = ''' 
            objectid AS id,
            addressid AS address_id,
            addrnopref AS address_number_prefix,
            addrno AS address_number,
            addrnosuff AS address_number_suffix,
            addrnosep AS address_number_separator,
            addrnocom AS address_number_common,
            stnameprd,
            stnameprm,
            stnameprt,
            stname AS street_name,
            stnamepot,
            stnamepod,
            stnamepom,
            stnamecom,
            subaddtype AS subaddress_type,
            subaddid AS subaddress_id,
            subaddelem,
            subaddcom,
            lndmrkname AS landmark_name,
            placename AS place_name,
            uspspn AS usps_place_name,
            uspspngnis AS gnis_place_id,
            uspsst AS usps_state,
            zip5 AS zipcode,
            gnismuni AS gnis_municipality_id,
            gnistwp AS gnis_township_id,
            gniscnty AS gnis_county_id,
            gnisstate AS gnis_state_id,
            uspsboxtyp AS usps_box_type,
            uspsboxid AS usps_box_id,
            uspsbox AS usps_box,
            addrdeliv AS delivery_address,
            cmpaddabrv AS complete_street_address,
            addrlastli AS city_state_zipcode,
            TRIM(COALESCE(LOWER(cmpaddabrv::VARCHAR), '')) || ' ' || 
            TRIM(COALESCE(LOWER(addrlastli::VARCHAR), '')) AS complete_address,
            xposition AS x_coordinate,
            yposition AS y_coordinate,
            longitude::double precision,
            latitude::double precision,
            usgridcord AS usng_address,
            pinsource AS pin_source,
            pin,
            anomaly,
            coordaccu AS coordinate_accuracy,
            univrsldt,
            editor,
            edittime AS edit_time,
            edittype AS edit_type,
            pwaeditor,
            pwaedtdate,
            pwa_commen AS edit_comment,
            pwa_status,
            geocode_mu AS geocode_municipality,
            document_s,
            comment
        '''

        create_all_addresses = ''' 
            CREATE TABLE cook_county_addresses AS (
                (SELECT {0} FROM chicago_addresses)
                UNION ALL
                (SELECT {0} FROM suburban_addresses)
            )
        '''.format(final_fields)

        self.executeTransaction(create_all_addresses)

        add_pk = ''' 
            ALTER TABLE cook_county_addresses ADD PRIMARY KEY (id)
        '''

        self.executeTransaction(add_pk)
        
class ChicagoETL(ETLThing):
    region_name = 'chicago'
    table_name = 'chicago_addresses'

class SuburbsETL(ETLThing):
    region_name = 'suburbs'
    table_name = 'suburban_addresses'

if __name__ == "__main__":
    import argparse
    from sqlalchemy import create_engine

    parser = argparse.ArgumentParser(
        description='Bulk load Cook County addresses into a PostgreSQL database.'
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
                        help="Pre-block addresses")

    args = parser.parse_args()
    
    if args.load_data:
        engine = create_engine('postgresql://localhost:5432/geocoder')
        connection = engine.connect()
        
        chicago = ChicagoETL(connection)
        chicago.run(download=args.download)

        suburbs = SuburbsETL(connection)
        suburbs.run(download=args.download)
        
        suburbs.mergeTables()

        connection.close()

    if args.train:
        from geocoder.deduper import DatabaseGazetteer
        import simplejson as json
        import dedupe

        engine = create_engine('postgresql://localhost:5432/geocoder')
        
        deduper = DatabaseGazetteer([{'field': 'complete_address', 'type': 'Address'}],
                                    engine=engine)

        messy_data = json.load(open('data/messy_addresses.json'))
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
        
        deduper.createMatchBlocksTable()
