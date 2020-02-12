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


class cm_queues(Resource):


    def get(self):
       # conn = getConnection  # connect to database
       # query = conn.execute("SELECT *  FROM networks.NETWORKS_CONCURRENT_QUEUES  WHERE CONCURRENT_QUEUE_ID in (21,22,23,5,13)")  # This line performs query and returns json result
       data  = get_cm_queues_stats()
       return jsonify({'data': data})


api.add_resource(cm_queues, '/cm_queues') # Route_1

if __name__ == '__main__':
    print("the  main")
    get_cm_queues_stats()
    app.run(
        debug=True,
        #host = '10.132.99.77',
        #port = 8050
    )
