import cx_Oracle
from flask import Flask, request,jsonify
from json import dumps
from flask_restful import Resource, Api
import pandas as pd
import logging
import sys

# using flask_restful

logging.basicConfig(stream=sys.stderr)

app =  Flask(__name__)
api = Api(app)


def getConnection():
    host = 'xxxxx/yyyy'  # hostaddr:port
    uname = 'xxxx'
    pw = 'ccc'
    constr=uname+'/'+pw+'@'+host
    connection = cx_Oracle.connect(constr,encoding = "UTF-8", nencoding = "UTF-8")
    return connection

def fetch_data(query):
    result = pd.read_sql(
        sql=query,
        con=getConnection()
    )
    return result


def get_cm_queues_stats():
    print("the  cm  starts")
    '''Returns the list of Queues status on manager that are currently running '''

    cm_queues_query = (
        f'''
        SELECT *  FROM networks.NETWORKS_CONCURRENT_QUEUES  WHERE CONCURRENT_QUEUE_ID in (21,22,23,5,13)      
        '''
    )
    cm_queues = fetch_data(cm_queues_query)
    cm_queues = list(cm_queues['CONCURRENT_QUEUE_NAME'].sort_values(ascending=True))
    print(" stats  " + cm_queues.__str__())
    return cm_queues

def get_concurentstates(status):
    print("the  cm  starts")
    '''Returns a  list currently running  process  as  per input  A<D<E<R'''

    concurrentstatus_query = (
        f'''
        SELECT queue_id, 
        request_id,
        argument1,
        STATUS_CODE, substr(CONCURRENT_PROGRAM,1,500)CONCURRENT_PROGRAM ,  
        to_char(REQUEST_DATE,'YYYY/MM/DD HH24:MI:SS') REQUEST_DATE, 
        to_char(requested_start_date,'YYYY/MM/DD HH24:MI:SS') requested_start_date,
        to_char(ACTUAL_START_DATE,'YYYY/MM/DD HH24:MI:SS') ACTUAL_START_DATE,
        to_char(ACTUAL_COMPLETION_DATE,'YYYY/MM/DD HH24:MI:SS')ACTUAL_COMPLETION_DATE,
        resubmit_interval, resubmit_interval_unit_code, resubmit_interval_type_code, completion_text     
        FROM NETWORKS.NETWORKS_CONCURRENT_REQUESTS
        WHERE QUEUE_ID = 5
        AND   STATUS_CODE = '{status}'
        ORDER BY requested_start_date desc   
        '''
    )
    results = fetch_data(concurrentstatus_query)



class cm_queues(Resource):
    def get(self):
       # conn = getConnection  # connect to database
       # query = conn.execute("SELECT *  FROM networks.NETWORKS_CONCURRENT_QUEUES  WHERE CONCURRENT_QUEUE_ID in (21,22,23,5,13)")  # This line performs query and returns json result
       data  = get_cm_queues_stats()
       return jsonify({'data': data})

class  Concurrent_status(Resource):
    def get(self):
        status = input("Please enter status code  to  filter  running  processes:\n")
        data = get_concurentstates(status)
        result = {'data': [dict(zip(tuple (data.keys()) ,i)) for i in data.cursor]}
        return jsonify(result)


api.add_resource(cm_queues, '/cm_queues') # Route_1
api.add_resource(Concurrent_status, '/cm_status/<status>') # Route_2

if __name__ == '__main__':
    print("the  main")
    get_cm_queues_stats()
    app.run(
        debug=True,
        #host = '10.132.99.77',
<<<<<<< HEAD
        port = 5002
    )
=======
        #port = 8050
    )
>>>>>>> 7b5147b2d33ccf5e791ea2d60c351f246e70d669
