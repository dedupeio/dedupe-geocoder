from flask import Blueprint, request, make_response, g
import json
from geocoder.deduper import GeocodingGazetteer
import os
import sqlalchemy as sa
from datetime import date
from collections import OrderedDict

api = Blueprint('api', __name__)

dthandler = lambda obj: obj.isoformat() if isinstance(obj, date) else None

@api.route('/geocode/')
def geocode():
    address = request.args.get('address')
    
    resp = {'status': 'ok', 'message': ''}
    status_code = 200

    if not address:
        resp['status'] = 'error'
        resp['message'] = 'address is required'
        status_code = 400
    
    if status_code == 200:
        match_blob = {'complete_address': address}
        
        settings_file = os.path.abspath(
                            os.path.join(
                                os.path.dirname(__file__), 
                                'dedupe.settings'))

        with open(settings_file, 'rb') as sf:
            matcher = GeocodingGazetteer(sf, engine=g.engine)
        
        matches = matcher.match(match_blob, n_matches=5, threshold=0.75)
        
        match_ids = []
        confidences = {}

        for match in matches:
            for link in match:
                (_, match_id), confidence = link
                match_ids.append(int(match_id))
                confidences[int(match_id)] = float(confidence)
        
        select_matches = ''' 
            SELECT * FROM cook_county_addresses
            WHERE id IN :match_ids
        '''
        
        match_records = []
    
        if match_ids:
            curs =  g.engine.execute(sa.text(select_matches), 
                                     match_ids=tuple(match_ids))
            for match in curs:
                m = OrderedDict(zip(match.keys(), match.values()))
                m['confidence'] = confidences[m['id']]
                match_records.append(m)
        
        resp['matches'] = match_records

        del matcher
    
    response = make_response(json.dumps(resp, default=dthandler))
    response.headers['Content-Type'] = 'application/json'
    return response
