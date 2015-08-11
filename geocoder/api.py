from flask import Blueprint, request, make_response, g
import json
from geocoder.deduper import StaticDatabaseGazetteer
import os
import sqlalchemy as sa
from datetime import date
from collections import OrderedDict

api = Blueprint('api', __name__)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None

@api.route('/')
def geocode():
    address = request.args['address']
    match_blob = {'complete_address': address}
    
    settings_file = os.path.abspath(os.path.join(os.path.dirname(__file__), 'dedupe.settings'))

    with open(settings_file, 'rb') as sf:
        matcher = StaticDatabaseGazetteer(sf, engine=g.engine)
    
    matches = matcher.match(match_blob, n_matches=5, threshold=0.75)
    
    match_ids = []
    confidence = 0
    for match in matches:
        for link in match:
            (_, match_id), confidence = link
            match_ids.append(int(match_id))

    match_records = ''' 
        SELECT * FROM cook_county_addresses
        WHERE id IN :match_ids
    '''
    
    if match_ids:
        match_records = [OrderedDict(zip(r.keys(), r.values())) for r in \
                             g.engine.execute(sa.text(match_records), 
                                              match_ids=tuple(match_ids))]
    else:
        match_records = []
    
    del matcher
    response = make_response(json.dumps(match_records, default=dthandler))
    response.headers['Content-Type'] = 'application/json'
    return response
