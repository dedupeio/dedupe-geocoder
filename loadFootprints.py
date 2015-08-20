import zipfile
import sqlalchemy as sa
import geoalchemy2 as ga2
import shapefile
from shapely.geometry import MultiPolygon, asShape
import csv
from geocoder.app_config import DB_USER, DB_PW, DB_HOST, \
    DB_PORT, DB_NAME
import psycopg2

DB_CONN_STR = 'host={0} dbname={1} user={2} port={3}'\
    .format(DB_HOST, DB_NAME, DB_USER, DB_PORT)

download_url = 'https://data.cityofchicago.org/api/geospatial/qv97-3bvb?method=export&format=Original'

def loadFootprints(path):

    with zipfile.ZipFile(path, 'r') as zf:
        for fname in zf.namelist():
            if fname.endswith('.shp'):
                zf.extract(fname, path='downloads')
                shp = open('downloads/%s' % fname, 'rb')
            if fname.endswith('.dbf'):
                zf.extract(fname, path='downloads')
                dbf = open('downloads/%s' % fname, 'rb')
            if fname.endswith('.shx'):
                zf.extract(fname, path='downloads')
                shx = open('downloads/%s' % fname, 'rb')
        
    shape_reader = shapefile.Reader(shp=shp, dbf=dbf, shx=shx)

    fields = shape_reader.fields[1:]
    
    GEO_TYPE_MAP = {
        'C': sa.String,
        'N': sa.Float,
        'L': sa.Float,
        'D': sa.TIMESTAMP,
        'F': sa.Float
    }

    columns = []
    for field in fields:
        fname, d_type, f_len, d_len = field
        col_type = GEO_TYPE_MAP[d_type]
        kwargs = {}
        
        if d_type == 'C':
            col_type = col_type(f_len)
        if fname == 'objectid':
            kwargs['primary_key'] = True

        columns.append(sa.Column(fname.lower(), col_type, **kwargs))

    geo_type = 'MULTIPOLYGON'
    columns.append(sa.Column('geom', ga2.Geometry(geo_type, srid=3435)))

    engine = sa.create_engine('postgresql://localhost:5432/geocoder', 
                           convert_unicode=True, 
                           server_side_cursors=True)

    table = sa.Table('temp_building_footprints', sa.MetaData(), *columns)
    
    table.drop(engine, checkfirst=True)
    table.create(engine)

    ins = table.insert()
    shp_count = 0
    values = []
    
    records = shape_reader.iterShapeRecords()
    
    with open('geocoder/data/building_footprints.csv', 'w') as f:
        writer = csv.DictWriter(f, table.columns.keys())
        writer.writeheader()

        for record in records:
            d = {}
            for k,v in zip(table.columns.keys(), record.record):
                try:
                    d[k] = v.decode('latin-1').replace(' ', '')
                except AttributeError:
                    d[k] = v
            try:
                geom = asShape(record.shape.__geo_interface__)
            except AttributeError as e:
                continue
            geom = MultiPolygon([geom])
            d['geom'] = 'SRID=3435;%s' % geom.wkt
            writer.writerow(d)
    
    copy_st = ''' 
        COPY temp_building_footprints 
        FROM STDIN 
        WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',')  
    '''

    with open('geocoder/data/building_footprints.csv', 'r') as f:
        conn = engine.raw_connection()

        try:
            cursor = conn.cursor()
            cursor.copy_expert(copy_st, f)
            conn.commit()
        except psycopg2.ProgrammingError as e:
            conn.rollback()
            raise e
        
        conn.close()

    engine.dispose()

def cleanupTable():

    create = ''' 
        CREATE TABLE building_footprints AS (
            SELECT 
              objectid::int AS id,
              bldg_id::int AS building_id,
              bldg_statu AS building_status,
              f_add1::int AS from_address,
              t_add1::int AS to_address,
              pre_dir1 AS street_direction,
              st_name1 AS street_name,
              st_type1 AS street_type,
              unit_name,
              non_standa AS non_standard,
              bldg_name1 AS building_name_1,
              bldg_name2 AS building_name_2,
              comments,
              stories::int,
              orig_bldg_::int AS original_building_id,
              footprint_ AS footprint_source,
              create_use AS create_user,
              bldg_creat AS building_creation_date,
              bldg_activ AS building_active_date,
              bldg_end_d AS building_end_date,
              demolished AS demolished_date,
              edit_date,
              edit_useri AS edit_user_id,
              edit_sourc AS edit_source,
              qc_date,
              qc_userid,
              qc_source,
              x_coord AS x_coordinate,
              y_coord AS y_coordinate,
              harris_str,
              no_of_unit::int AS number_of_units,
              no_stories::int AS number_of_stories,
              year_built::int AS year_built,
              bldg_sq_fo::int AS building_sqaure_footage,
              bldg_condi AS building_condition,
              condition_ AS condition_date,
              vacancy_st AS vacancy_status,
              label_hous AS label_house,
              suf_dir1 AS street_suffix,
              shape_area,
              shape_len,
              NULL::VARCHAR AS complete_address,
              NULL::VARCHAR AS address_id,
              ST_Transform(geom, 4326) AS geom
            FROM temp_building_footprints
        )
    '''

    engine = sa.create_engine('postgresql://localhost:5432/geocoder', 
                           convert_unicode=True, 
                           server_side_cursors=True)
    
    with engine.begin() as conn:
        conn.execute('DROP TABLE IF EXISTS building_footprints')
        conn.execute(create)

    with engine.begin() as conn:
        conn.execute("SELECT UpdateGeometrySRID('building_footprints', 'geom', 4326)")
        conn.execute("CREATE INDEX footprint_geom_idx ON building_footprints USING GIST (geom)")

    add_complete = ''' 
        UPDATE building_footprints SET
          complete_address = subq.complete_address
        FROM (
            SELECT
              (COALESCE(from_address::varchar, '') || ' ' || 
               COALESCE(street_direction, '') || ' ' ||
               COALESCE(street_name, '') || ' ' ||
               COALESCE(street_type, '') || ' ' ||
               'Chicago, IL'
              ) AS complete_address,
              id
            FROM building_footprints
            WHERE from_address = to_address
              AND from_address != 0
        ) AS subq 
        WHERE building_footprints.id = subq.id
    '''
    
    with engine.begin() as conn:
        conn.execute(add_complete)

    add_complete = ''' 
        UPDATE building_footprints SET
          complete_address = subq.complete_address
        FROM (
            SELECT
              (COALESCE(from_address::varchar, '') || '-' || 
               right(COALESCE(to_address::varchar, ''), 2) || ' ' ||
               COALESCE(street_direction, '') || ' ' ||
               COALESCE(street_name, '') || ' ' ||
               COALESCE(street_type, '') || ' ' ||
               'Chicago, IL'
              ) AS complete_address,
              id
            FROM building_footprints
            WHERE from_address != to_address
              AND from_address != 0
        ) AS subq 
        WHERE building_footprints.id = subq.id
    '''
    
    with engine.begin() as conn:
        conn.execute(add_complete)

    add_pk = ''' 
        ALTER TABLE building_footprints ADD PRIMARY KEY (id)
    '''

    with engine.begin() as conn:
        conn.execute(add_pk)


if __name__ == "__main__":
    # loadFootprints('downloads/chicago_building_footprints.zip')
    cleanupTable()
