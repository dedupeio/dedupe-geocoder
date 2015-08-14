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

if __name__ == "__main__":
    import argparse
    from sqlalchemy import create_engine
    from geocoder.data_loader import ETLThing
    import sys

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

        connection.close()

    if args.train:
        from geocoder.deduper import DatabaseGazetteer
        import simplejson as json
        import dedupe

        engine = create_engine('postgresql://localhost:5432/geocoder')
        
        deduper = DatabaseGazetteer([{'field': 'complete_address', 'type': 'Address'}],
                                    engine=engine)
        
        sql_table = checkForTable(engine, args.name)
        
        if sql_table == None:
            sys.exit()

        primary_key = sql_table.primary_key.columns.keys()[0]
        
        messy_table = ''' 
            SELECT {0}, complete_address
            FROM {1}
        '''.format(primary_key, args.name)

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

    if args.block:
        from geocoder.deduper import StaticDatabaseGazetteer

        engine = create_engine('postgresql://localhost:5432/geocoder')
        
        with open('geocoder/data/dedupe.settings', 'rb') as sf:
            deduper = StaticDatabaseGazetteer(sf, engine=engine)
        
        # Re-block the county addresses table in light of the newly trained
        # settings file
        deduper.createMatchBlocksTable()
        
        # Block the new table, too
        deduper.createMatchBlocksTable(table_to_block=args.name,
                                       match_blocks_table='%s_match_blocks' % args.name)

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

        matches = deduper.match(messy_data_info, n_matches=5, threshold=0.75)
        
        # From here, take the matches and update the messy data source with 
        # canonical IDs. Or something ...
        print(matches[0])
