import os
import sqlalchemy as sa


def checkForTable(engine, table_name):
    try:
        sql_table = sa.Table(args.name, 
                             sa.MetaData(), 
                             autoload=True, 
                             autoload_with=engine)
        return sql_table
    except sa.exc.NoSuchTableError as e:
        meta = sa.MetaData()
        meta.reflect(bind=engine)
        tables = list(meta.tables.keys())
        print('Table %s not found amongst tables in database: %s' \
                % (args.name, ','.join(tables)))
        return None

def trainIncoming(name):
    from geocoder.deduper import DatabaseGazetteer
    import simplejson as json
    import dedupe

    engine = create_engine('postgresql://localhost:5432/geocoder')
    
    deduper = DatabaseGazetteer([{'field': 'complete_address', 'type': 'Address'}],
                                engine=engine)
    
    sql_table = checkForTable(engine, name)
    
    if sql_table == None:
        sys.exit()

    primary_key = sql_table.primary_key.columns.keys()[0]
    
    messy_table = ''' 
        SELECT {0}, complete_address
        FROM {1}
        WHERE address_id IS NULL
    '''.format(primary_key, name)

    curs = engine.execute(messy_table)

    messy_data = ({'complete_address': r.complete_address} for r in curs)

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
    with open('geocoder/dedupe.settings', 'wb') as sf :
        deduper.writeSettings(sf)

    deduper.cleanupTraining()

def blockIncoming(name, train):
    from geocoder.deduper import StaticDatabaseGazetteer

    engine = create_engine('postgresql://localhost:5432/geocoder')
    
    with open('geocoder/data/dedupe.settings', 'rb') as sf:
        deduper = StaticDatabaseGazetteer(sf, engine=engine)
    
    # If we trained, re-block the county addresses table 
    # in light of the newly trained settings file
    if train:
        deduper.createMatchBlocksTable()
    
    # Block the new table, too
    deduper.createMatchBlocksTable(table_to_block=name,
                                   match_blocks_table='%s_match_blocks' % name)


if __name__ == "__main__":
    import argparse
    from sqlalchemy import create_engine
    from geocoder.data_loader import ETLThing
    import sys
    import csv

    parser = argparse.ArgumentParser(
        description='Bulk link data to Cook County address data.'
    )
    
    parser.add_argument('--name', 
                        type=str, 
                        help='Give this job a name',
                        required=True)

    parser.add_argument('--download', 
                        type=str, 
                        help='Download location for the data you want to link')

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
        
        primary_key = input('What is the primary key? (It\'s OK if there isn\'t one) ')
        if primary_key.strip() == '':
            primary_key = None
        
        while True:
            address_fields = input('What fields are the address components stored in? (comma separated list) ')
            if address_fields.strip() == '':
                print('address_fields is required')
            else:
                address_fields = address_fields.split(',')
                break
        
        engine = create_engine('postgresql://localhost:5432/geocoder')
        connection = engine.connect()
        
        etl = ETLThing(connection, 
                       args.name,
                       primary_key=primary_key,
                       address_fields=address_fields)
        
        etl.run(download_url=args.download, messy=True)

        add_address_id = ''' 
            ALTER TABLE {0} ADD COLUMN address_id VARCHAR
        '''.format(args.name)
        
        add_match_confidence = ''' 
            ALTER TABLE {0} ADD COLUMN match_confidence DOUBLE PRECISION
        '''.format(args.name)
        
        etl.executeTransaction(add_address_id)
        etl.executeTransaction(add_match_confidence)

        connection.close()

    if args.train:
        trainIncoming(args.name)

    if args.block:
        blockIncoming(args.name, args.train)

    if args.link:
        from geocoder.deduper import AddressLinkGazetteer

        engine = create_engine('postgresql://localhost:5432/geocoder')
        
        sql_table = checkForTable(engine, args.name)
        
        if sql_table == None:
            sys.exit()
        
        primary_key = sql_table.primary_key.columns.keys()[0]
        
        with open('geocoder/dedupe.settings', 'rb') as sf:
            deduper = AddressLinkGazetteer(sf, engine=engine)
        
        messy_data_info = {
            'messy_data_table': args.name,
            'messy_blocks_table': '%s_match_blocks' % args.name,
            'primary_key': primary_key,
        }

        matches = deduper.match(messy_data_info, n_matches=5)
        
        with open('geocoder/data/%s_matches.csv' % args.name, 'w') as f:
            writer = csv.writer(f)
            for match in matches:
                for link in match:
                    (messy_id, canonical_id), confidence = link
                    if float(confidence) > 0.8:
                        writer.writerow([int(messy_id), 
                                         int(canonical_id), 
                                         float(confidence)])

        temp_matches_name = '{0}_temp_matches'.format(args.name)
        temp_matches_table = ''' 
            CREATE TABLE {0} (
                messy_id INTEGER,
                canonical_id INTEGER,
                confidence DOUBLE PRECISION
            )
        '''.format(temp_matches_name)

        with engine.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS {0}'.format(temp_matches_name))
            conn.execute(temp_matches_table)
        
        import psycopg2
        from geocoder.app_config import DB_USER, DB_PW, DB_HOST, \
            DB_PORT, DB_NAME
        
        DB_CONN_STR = 'host={0} dbname={1} user={2} port={3}'\
            .format(DB_HOST, DB_NAME, DB_USER, DB_PORT)

        copy_st = ''' 
            COPY {0} FROM STDIN WITH (FORMAT CSV, DELIMITER ',')
        '''.format(temp_matches_name)
        
        with open('geocoder/data/%s_matches.csv' % args.name, 'r') as f:
            next(f)
            with psycopg2.connect(DB_CONN_STR) as conn:
                with conn.cursor() as curs:
                    try:
                        curs.copy_expert(copy_st, f)
                    except psycopg2.IntegrityError as e:
                        print(e)
                        conn.rollback()

        update_records = ''' 
            UPDATE {0} SET
              address_id = subq.address_id,
              match_confidence = subq.confidence
            FROM (
              SELECT
                c.address_id,
                t.messy_id,
                t.confidence
              FROM cook_county_addresses AS c
              JOIN {1} AS t
                ON c.id = t.canonical_id
            ) AS subq
            WHERE {0}.id = subq.messy_id
        '''.format(args.name, temp_matches_name)

        with engine.begin() as conn:
            records = conn.execute(update_records)
        
        print('Saved: %s records' % records.rowcount)

        while True:
            unlinked_records = ''' 
                SELECT COUNT(*) AS record_count
                FROM {0}
                WHERE address_id IS NULL
            '''.format(args.name)

            unlinked_count = engine.execute(unlinked_records)\
                                 .first().record_count

            retrain = input('''There are {0} records that were not matched. 
Would you like to retrain with these records? (y or n) '''.format(unlinked_count))

            if retrain == 'y':
                trainIncoming(args.name)
                blockIncoming(args.name, True)
            else:
                break
