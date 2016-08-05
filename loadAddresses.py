import requests
import zipfile
from simpledbf import Dbf5
import re
import os
import sqlalchemy as sa
from geocoder.data_loader import ETLThing
from geocoder.app_config import DB_CONN

class CookCountyETL(ETLThing):

    def download(self, download_url=None):
        
        addresses = requests.get(download_url, stream=True)
        
        with open(self.zip_file_path, 'wb') as f:
            for chunk in addresses.iter_content(chunk_size=1024):
                if chunk:
                    f.write(chunk)
                    f.flush()
    
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
        
        # This is a little stupid since we are just loading chicago
        # addresses but I am lazy and did not want to rewrite stuff.
        create_all_addresses = ''' 
            CREATE TABLE cook_county_addresses AS (
                SELECT {0} FROM chicago_addresses
            )
        '''.format(final_fields)

        self.executeTransaction(create_all_addresses)

        add_pk = ''' 
            ALTER TABLE cook_county_addresses ADD PRIMARY KEY (id)
        '''

        self.executeTransaction(add_pk)

        pin_index = ''' 
            CREATE INDEX pin_idx ON cook_county_addresses (pin)
        '''

        self.executeTransaction(pin_index)
        
        pin_index = ''' 
            CREATE INDEX address_id_idx ON cook_county_addresses (address_id)
        '''

        self.executeTransaction(pin_index)

class ChicagoETL(CookCountyETL):
    region_name = 'chicago'
    table_name = 'chicago_addresses'
    zip_file_path = 'downloads/chicago_addresses.zip'
    four_by_four = 'jev2-4wjs'

class SuburbsETL(CookCountyETL):
    region_name = 'suburbs'
    table_name = 'suburban_addresses'
    zip_file_path = 'downloads/suburbs_addresses.zip'
    four_by_four = '6mf5-x8ic'

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
    
    cook_county_data_portal = 'https://datacatalog.cookcountyil.gov/api/geospatial/%s?method=export&format=Original'

    if args.load_data:
        engine = create_engine(DB_CONN)
        connection = engine.connect()
        
        chicago = ChicagoETL(connection, 'chicago_addresses')
        download_url = None
        
        if args.download:
            download_url = cook_county_data_portal % chicago.four_by_four
        
        chicago.run(download_url=download_url)
        chicago.mergeTables()
        
        # Skipping the suburbs for now
        # suburbs = SuburbsETL(connection, 'suburban_addresses')
        # 
        # if args.download:
        #     download_url = cook_county_data_portal % suburbs.four_by_four
        # 
        # suburbs.run(download_url=download_url)
        # 
        # suburbs.mergeTables()

        connection.close()

    if args.train:
        from geocoder.deduper import DatabaseGazetteer
        import simplejson as json
        import dedupe

        from geocoder.app_config import DB_CONN
        engine = create_engine(DB_CONN) 
        
        deduper = DatabaseGazetteer([{'field': 'complete_address', 'type': 'Address'}],
                                    engine=engine)

        messy_data = json.load(open('geocoder/data/messy_addresses.json'))
        deduper.drawSample(messy_data, sample_size=30000)
        
        if os.path.exists('geocoder/data/training.json'):
            print('reading labeled examples from geocoder/data/training.json')
            with open('geocoder/data/training.json') as tf :
                deduper.readTraining(tf)
        
        dedupe.consoleLabel(deduper)

        deduper.train(ppc=0.1, index_predicates=False)
        
        # When finished, save our training away to disk
        with open('geocoder/data/training.json', 'w') as tf :
            deduper.writeTraining(tf)

        # Save our weights and predicates to disk.  If the settings file
        # exists, we will skip all the training and learning next time we run
        # this file.
        with open('geocoder/data/dedupe.settings', 'wb') as sf :
            deduper.writeSettings(sf)

        deduper.cleanupTraining()

    if args.block:
        from geocoder.deduper import StaticDatabaseGazetteer

        engine = create_engine(DB_CONN)
        
        with open('geocoder/data/dedupe.settings', 'rb') as sf:
            deduper = StaticDatabaseGazetteer(sf, engine=engine)
        
        deduper.createMatchBlocksTable()
