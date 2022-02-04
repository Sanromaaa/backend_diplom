import mysql.connector
import threading
import requests
import argparse

from datetime import datetime
from operator import attrgetter
from utils import config_parser
from flask import Flask, request, jsonify
from flask_cors import CORS


class Server:

    def __init__(self, host, port):
        self.host = host
        self.port = port

        #        self.db_host = db_host
        #        self.db_port = db_port
        #        self.db_user = db_user
        #        self.db_password = db_pass
        #        self.db_name = db_name

        self.app = Flask(__name__)
        self.app.add_url_rule('/shutdown', view_func=self.shutdown)
        self.app.add_url_rule('/home', view_func=self.get_data)
        self.app.add_url_rule('/push', view_func=self.push_data)
        self.app.add_url_rule('/js', view_func=self.json_data)
        self.app.add_url_rule('/check', view_func=self.check_conf)
        self.app.add_url_rule('/create', view_func=self.create_db)
        self.app.add_url_rule('/', view_func=self.get_home)

        CORS(self.app, resources={
            r"/*": {"origins": "*"}
        })

    def run_server(self):
        self.server = threading.Thread(target=self.app.run, kwargs={'host': self.host, 'port': self.port})
        self.server.start()
        return self.server

    #    def shutdown_server(self):
    #        request.get(f'http://{self.host}:{self.port}/shutdown')

    def json_data(self):
        #        x = DbData + 10
        return '{x: 10}'

    def get_data(self):
        mydb = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            port=db_port,
            database=db_name
        )
        fromdb = mydb.cursor()
        fromdb.execute('select * from test_db.date')
        #        return server.json_data(10)
        DBdate = fromdb.fetchall()
        return jsonify(DBdate)

    def create_db(self):
        mydb = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            port=db_port,
#            database=db_name
        )
        fromdb = mydb.cursor()

        try:
            fromdb.execute('create database test_db;')
            fromdb.execute('create table test_db.date ('
                           'ID int not null auto_increment, YEAR int not null, MONTH int not null, DAY int not null, '
                           'HOUR int not null, MINUTE int not null, SECOND int not null, '
                           'MICROSECOND int not null, PRIMARY KEY (ID));')
        except:
            mydb.rollback()
        return 'all OK'

    def push_data(self):
        mydb = mysql.connector.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            port=db_port,
            database=db_name
        )
        fromdb = mydb.cursor()
        attrs = ('year', 'month', 'day', 'hour', 'minute', 'second', 'microsecond')
        sql = "insert into test_db.date (year, month, day, hour, minute, second, microsecond) values (%s, %s, %s, %s, %s, %s, %s)"
        current_time = datetime.now()
        dates = attrgetter(*attrs)(current_time)
        try:
            fromdb.execute(sql, dates)
            mydb.commit()
        except:
            mydb.rollback()
        return 'all OK', 201

    def shutdown(self):
        terminate_func = request.environ.get('werkzeug.server.shutdown')
        if terminate_func:
            terminate_func()

    def get_home(self):
        return db_host

    def check_conf(self):
        return (db_password)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', type=str, dest='config')

    args = parser.parse_args()

    config = config_parser(args.config)

    server_host = config['SERVER_HOST']
    server_port = config['SERVER_PORT']

    db_host = config['DB_HOST']
    db_port = config['DB_PORT']
    db_user = config['DB_USER']
    db_password = config['DB_PASSWORD']
    db_name = config['DB_NAME']

    server = Server(
        host=server_host,
        port=server_port,
        #        db_port=db_port,
        #        db_host=db_host,
        #        db_pass=db_password,
        #        db_user=db_user,
        #        db_name=db_name
    )

    server.run_server()
