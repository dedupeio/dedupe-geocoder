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
            SELECT id, complete_address,
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
    
    def createMatchBlocksTable(self):
        
        with self.engine.begin() as conn:
            conn.execute('DROP TABLE IF EXISTS match_blocks')
            conn.execute(''' 
                CREATE TABLE match_blocks (
                    block_key VARCHAR, 
                    id INTEGER
                )
                ''')

        sel = ''' 
            SELECT id, complete_address
            FROM cook_county_addresses
        '''

        with self.engine.connect() as read_conn :
            rows = read_conn.execute(sel)
            data = ((row['id'], dict(row)) for row in rows) 
            block_gen = self.blocker(data)

            ins = '''
                INSERT INTO match_blocks 
                     (block_key, id) 
                     VALUES (%s, %s)
                '''

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
                DROP INDEX IF EXISTS match_blocks_key_idx
            ''')
        
        with self.engine.begin() as conn:
            conn.execute('''
                CREATE INDEX "match_blocks_key_idx" 
                  ON match_blocks (block_key)
            ''')
        
        self.engine.dispose()

