from flask import Flask, render_template, request
from flask_restful import Resource, Api
from datetime import datetime
import json
import requests
import os
import re
import shutil
from ihepcdatabase import db_session
from ihepcmodels import PowerConsumption
import ihepc_helpers as ih
import marshmallow as lo
from marshmallow import ValidationError, post_load
from sqlalchemy.sql import text

app = Flask(__name__)
app.config.from_pyfile('config.cfg')

api = Api(app)

@app.teardown_appcontext
def shutdown_session(exception=None):
    db_session.remove()

class PCMalloSchema(lo.Schema):
    id = lo.fields.Int()
    date = lo.fields.Date('%Y-%m-%d')
    date_time = lo.fields.DateTime('%Y-%m-%dT%H:%M:%S+00:00')
    global_active_power = lo.fields.Float()
    global_reactive_power = lo.fields.Float()
    voltage = lo.fields.Float()
    global_intensity = lo.fields.Float()
    sub_metering_1 = lo.fields.Float()
    sub_metering_2 = lo.fields.Float()
    sub_metering_3 = lo.fields.Float()
    line_no = lo.fields.Float()
    @post_load
    def make_power_consumption(self, data):
        return PowerConsumption(**data)

def build_filter_clause(attribute, value):
    operator_mapping = {'eq':'=','gte':'>=','lte':'<=','gt':'>','lt':'<'}
    op_va_re = re.compile('^(eq|gte|lte|gt|lt):(.*$)')
    op, value = op_va_re.match(value).groups()
    op = operator_mapping[op]
    # consider if we can introspect PowerConsumption and get data type for column
    if attribute == 'date':
        value = "'%s'" % (value)
    return "%s %s %s" % (attribute, op, value)

class Query(Resource):
    # read/read/query
    def get(self):
        attribute_names = ('id', 'date', 'global_active_power', 'global_reactive_power', 
                   'voltage', 'global_intensity', 'sub_metering_1', 'sub_metering_2', 'sub_metering_3')
        
        allowable_args = {k:v for k,v in request.args.items() if k in attribute_names}
        filters = [build_filter_clause(k,v) for k,v in allowable_args.items()]
        filter_string = " and ".join(filters)
        q_results = db_session.query(PowerConsumption).filter(text(filter_string))
        pc_converter = PCMalloSchema(many=True)
        j_results = pc_converter.dump(q_results.all())
        return j_results, 200

class IHEPC(Resource):
    # create
    def post(self):
        if request.json:
           j = json.loads(request.json)
           print (type(j))
           print (j)
           pc_converter = PCMalloSchema()
           try:
               print ("LOADING ----------------")
               new_pc = pc_converter.load(j)
               print ("ADDING TO SESSION ----------------")
               print (type(new_pc))
               db_session.add(new_pc)
               print ("COMMITING ----------------")
               db_session.commit()
               print ("COMMITTED ----------------")
           except ValidationError as err:
               print ("ValidationError ----------------")
               print (err.messages)
               print (err.valid_data)
           print(new_pc)
           return 200
        return 400
    # update
    def put(self, id):
        q = db_session.query(PowerConsumption).filter(PowerConsumption.id == id)
        j = json.loads(request.json)
        r = q.all()
        if len(r) == 1:
            r = r[0]    
            if 'global_active_power' in j:
                r.global_active_power = j['global_active_power']
            if 'global_intensity' in j:
                r.global_intensity = j['global_intensity']
            if 'global_reactive_power' in j:
                r.global_reactive_power = j['global_reactive_power']
            if 'voltage' in j:
                r.voltage = j['voltage']
            if 'sub_metering_1' in j:
                r.sub_metering_1 = j['sub_metering_1']
            if  'sub_metering_1' in j:
                r.sub_metering_2 = j['sub_metering_2']
            if  'sub_metering_1' in j:
                r.sub_metering_3 = j['sub_metering_3']

            db_session.commit()
            return 200
        return 400
    # delete
    def delete(self, id):
        q = db_session.query(PowerConsumption).filter(PowerConsumption.id == id)
        rows_deleted = q.delete()
        print ("DELETED #",rows_deleted)
        db_session.commit()
        if rows_deleted == 1:
            return 200        
        return 404

class Refresh(Resource):
    def delete(self):
        from_url = app.config['DATA_SOURCE_URL']
        download_results = ih.download(from_url, app.config['DIR_INBOUND'])
        fileid = download_results['fileid']
         
        in_f  = os.path.join(app.config['DIR_INBOUND'], fileid)
        processing_f = os.path.join(app.config['DIR_PROCESSING'], fileid)
        archive_f = os.path.join(app.config['DIR_ARCHIVE'], fileid)

        # Move file to processing, load, move to archive
        shutil.move(in_f, processing_f)

        ih.truncate(db_session)
        ih.load(processing_f, db_session)

        shutil.move(processing_f, archive_f)
        return {'action':'refresh', 'fileid':fileid, 'message':'refreshed ihepc.db'}, 200

api.add_resource(Refresh, "/v1.0/ihepc/refresh")
api.add_resource(Query, "/v1.0/ihepc")
api.add_resource(IHEPC, "/v1.0/ihepc/<int:id>","/v1.0/ihepc")

if __name__ == '__main__':
    app.config.from_object('config')
    app.config.from_pyfile('config.cfg')
    app.run()
