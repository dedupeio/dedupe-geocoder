import dedupe
from dedupe import StaticGazetteer, Gazetteer
import re
import sqlalchemy as sa

class DatabaseGazetteer(Gazetteer):
    ''' 
    This is used to get sample, train and save settings file
    '''
    def __init__(self, *args, **kwargs):

        self.engine = kwargs['engine']

        del kwargs['engine']

        super(DatabaseGazetteer, self).__init__(*args, **kwargs)

    def drawSample(self, messy_data, sample_size=2500):
        ''' 
        Make a sample from canonical and messy data
        '''
        
        messy_dict = dict((idx, {'complete_address': row['complete_address'].lower()}) \
                for idx, row in enumerate(messy_data))
        
        canonical_records = ''' 
            SELECT id, complete_address
            FROM cook_county_addresses
            WHERE complete_address IS NOT NULL
            ORDER BY RANDOM()
        '''.format(sample_size)
        
        curs = self.engine.execute(canonical_records)
        canonical_dict = dict(
            (row.id, dedupe.core.frozendict({'complete_address': row.complete_address}))
            for row in curs
        )
        
        self.sample(messy_dict, canonical_dict, 
                    sample_size=sample_size, 
                    blocked_proportion=1)


class StaticDatabaseGazetteer(StaticGazetteer):
    
    ''' 
    This is used to actually perform matches
    '''

    def __init__(self, *args, **kwargs):

        self.engine = kwargs['engine']
        
        del kwargs['engine']

        super(StaticDatabaseGazetteer, self).__init__(*args, **kwargs)
    
    def preProcess(self, column):
        if column :
            column = str(column)
            column = re.sub('  +', ' ', column)
            column = re.sub('\n', ' ', column)
            column = column.strip().strip('"').strip("'").lower().strip()
            if not column :
                column = ''
        else :
            column = ''

        return column


    
    def createMatchBlocksTable(self, 
                               table_to_block='cook_county_addresses', 
                               match_blocks_table='match_blocks',
                               primary_key='id',
                               address_field='complete_address'):
        
        with self.engine.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS {0}'.format(match_blocks_table))
            conn.execute(''' 
                CREATE TABLE {0} (
                    block_key VARCHAR, 
                    {1} INTEGER
                )
                '''.format(match_blocks_table, primary_key))

        sel = ''' 
            SELECT 
              {0}, 
              {1} AS complete_address
            FROM {2}
        '''.format(primary_key,
                   address_field,
                   table_to_block)

        with self.engine.connect() as read_conn :
            rows = read_conn.execute(sel)
            data = ((row['id'], dict(row)) for row in rows) 
            block_gen = self.blocker(data)

            ins = '''
                INSERT INTO {0} 
                     (block_key, {1}) 
                     VALUES (%s, %s)
                '''.format(match_blocks_table, primary_key)

            write_conn = self.engine.raw_connection()
            curs = write_conn.cursor()

            try :
                for block in block_gen :
                    curs.execute(ins, block)
                write_conn.commit()
            except : # pragma: no cover
                write_conn.rollback()
                raise
            finally :
                curs.close()
                write_conn.close()

        with self.engine.begin() as conn:
            conn.execute('''
                DROP INDEX IF EXISTS {0}_key_idx
            '''.format(match_blocks_table))
        
        with self.engine.begin() as conn:
            conn.execute('''
                CREATE INDEX {0}_key_idx 
                  ON {0} (block_key)
            '''.format(match_blocks_table))
        
        self.engine.dispose()

class AddressLinkGazetteer(StaticDatabaseGazetteer):
    
    def _blockData(self, messy_data):

        ''' 
        `messy_data` should be a dict like so:

        {
            'messy_data_table': '<table_name>',
            'messy_blocks_table': '<table_name>',
        }

        optionally one can add `primary_key` to indicate
        how it are stored in the messy_data_table
        '''

        primary_key = messy_data.get('primary_key', 'id')

        messy_blocks = ''' 
            SELECT 
              blocks.{0} AS blocked_record_id,
              array_agg(blocks.block_key) AS block_keys,
              MAX(messy_data.complete_address) AS complete_address
            FROM {1} AS blocks
            JOIN {2} AS messy_data
              USING({0})
            GROUP BY {0}
        '''.format(primary_key,
                   messy_data['messy_blocks_table'], 
                   messy_data['messy_data_table'])
        
        canon_blocks = ''' 
            SELECT
              DISTINCT ON (addresses.id)
              addresses.id,
              addresses.complete_address
            FROM cook_county_addresses AS addresses
            JOIN match_blocks AS blocks
              USING(id)
            WHERE blocks.block_key IN :block_keys
            ORDER BY addresses.id
        '''

        curs = self.engine.execute(sa.text(messy_blocks))

        for messy_record in curs:
            record = {k:v for k,v in zip(messy_record.keys(), messy_record.values()) \
                          if k not in ['blocked_record_id', 'block_keys']}

            A = [(messy_record.blocked_record_id, record, set())]
            
            rows = self.engine.execute(sa.text(canon_blocks), 
                                       block_keys=tuple(messy_record.block_keys))

            B = [(row.id, row, set()) for row in rows]

            if B:
                yield (A, B)


class GeocodingGazetteer(StaticDatabaseGazetteer):
    
    def _blockData(self, messy_data):
        
        address = self.preProcess(messy_data['complete_address'])
        
        record = {'complete_address': address}
        
        block_keys = {b[0] for b in \
            list(self.blocker([('messy', record)]))}

        # Distinct by record id where records exist in entity_map
        sel = ''' 
            SELECT
              DISTINCT ON (addresses.id)
              addresses.id,
              addresses.complete_address
            FROM cook_county_addresses AS addresses
            JOIN match_blocks AS blocks
              USING(id)
            WHERE blocks.block_key IN :block_keys
            ORDER BY addresses.id
        '''
        
        B = []

        local_engine = self.engine.execute

        A = [('messy', record, set())]
        rows = local_engine(sa.text(sel), 
                block_keys=tuple(block_keys),
                state='accepted')
        B = [(row.id, row, set()) for row in rows]
        
        if B:
            yield (A,B)

