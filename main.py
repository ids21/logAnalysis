import re
import sys
import IP2Location
import dateutil.parser
import dateutil.tz
import os
import psycopg2
import plotly.graph_objects as go
import numpy as np
import pandas as pd
from contextlib import closing
from psycopg2.extras import DictCursor

database = IP2Location.IP2Location(os.path.join("IP2LOCATION-LITE-DB5.BIN"))

logline = '([(\d\.)]+) (.*) (.*) \[(.*?)\] "(.*?)" (\d+) (\d+) (.*?)$'
UTC_ZONE = dateutil.tz.gettz('UTC')

log_timestamp =re.compile(
    '([0-9]+)'  # day
    '[/]'
    '([A-Za-z]+)'  # month
    '[/]'
    '([0-9]+)'  # year
    '[:]'
    '([0-9]+)'  # hour
    '[:]'
    '([0-9]+)'  # minute
    '[:]'
    '([0-9]+)'  # second
    '\s+'
    '([^\s]+)'  # timezone
)
request = re.compile(
    '([^\s]+)'  # method
    '\s+'
    '(.+)'  # path
    '\s+'
    '(HTTP [0-9].[0-9])'  # http version
)

def get_datetime(timestamp_wtz):
    day, month, year, hour, minute, second, tz =\
            log_timestamp.findall(timestamp_wtz)[0]
    fmt = '%s %s %s %s:%s:%s %s' % (month, day, year, hour, minute, second, tz)
    dt = dateutil.parser.parse(fmt)
    dt = dt.astimezone(UTC_ZONE)
    return dt

def parse_request_string(request_line):
    method, path, version = '', '', ''
    try:
        method, path, version = request.findall(request_line)[0]
    except IndexError:
        pass
    return method, path, version

def parse_line(filepath):
    try:
        for line in open(filepath):
            line = line.strip()
            ip,_ ,_ , timestamp, request, status, length, userID = re.match(logline, line).groups()
            timestamp = str(get_datetime(timestamp))
            method, path, version = parse_request_string(request) or str(0)
            status = (status and int(status)) or str(0)
            length = (length != '-' and int(length)) or 0
            userID = userID or None

            record = {
                'ip': ip,
                'timestamp':timestamp,
                'method':method,
                'path':path,
                'version':version,
                'status':status,
                'length':length,
                'userID':userID,
            }
            yield record
            
    except Exception as e:
        print('В этой строке произошла ошибка {}'.format(line))
        
def createTable():
    try:
        conn = psycopg2.connect(host="localhost", database="LogPython", password='postgres', user="postgres")
        cursor = conn.cursor()
        cursor.execute("DROP TABLE IF EXISTS LOG")
        cursor.execute("""CREATE TABLE LOG
                 (ip TEXT NOT NULL,
                  timestamp timestamp with time zone NOT NULL,
                  method TEXT DEFAULT '-' NOT NULL,
                  path TEXT DEFAULT '-' NOT NULL,
                  version TEXT DEFAULT '-' NOT NULL,
                  status VARCHAR(5) DEFAULT '-' NOT NULL,
                  length VARCHAR(5) DEFAULT '-' NOT NULL,
                  userID VARCHAR(7) DEFAULT '-' NOT NULL)
                  """)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



def fillTable(filepath):
    try:
        conn = psycopg2.connect(host="localhost", database="LogPython", password='postgres', user="postgres")
        cursor = conn.cursor()
        for i, record in enumerate(parse_line(filepath)):
            cursor.execute("""
            INSERT INTO LOG (ip,timestamp,method,path,version,status,length,userID)
             values (%s,%s,%s,%s,%s,%s,%s,%s)""",\
                (record.get('ip'), \
                (record.get('timestamp')),\
                (record.get('method')),\
                (record.get('path')),\
                (record.get('version')),\
                (record.get('status')),\
                (record.get('length')),\
                (record.get('userID'))))
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

def userPerMonth():
    try:
        with closing(psycopg2.connect(host="localhost", database="LogPython", password='postgres', user="postgres")) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""SELECT DATE_PART('month',timestamp), COUNT(DISTINCT userID)
                                 from log group by DATE_PART('month',timestamp);""")
                for row in cursor:
                    print(row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def orderPerDay():
    try:
        with closing(psycopg2.connect(host="localhost", database="LogPython", password='postgres', user="postgres")) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""select distinct date(timestamp),count(userID) over(partition by date(timestamp))
from log where substring(path,2,5) = 'order';""")
                for row in cursor:
                    print(row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def purchase():
    try:
        with closing(psycopg2.connect(host="localhost", database="LogPython", password='postgres', user="postgres")) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                with post as (select min(t.timestamp) as time ,t.user_agent from log t
where substring(path,2,7) = 'catalog' group by userID),
get as (select MIN(timestamp) AS first_purchase,userID from log where substring(path,2,5) = 'order' 
group by userID),
diff_time as (select get.ID, DATE_PART('hour', get.first_purchase - post.time)*60 + 
DATE_PART('minute', get.first_purchase - post.time) diff 
		from get
		inner join post on
		post.userID = get.userID)
select MIN(DIFF),MAX(DIFF),AVG(DIFF) from diff_time
""")
                for row in cursor:
                    print(row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def minmaxavg():
    try:
        with closing(psycopg2.connect(host="localhost", database="LogPython", password='postgres', user="postgres")) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""with countByPage as(select count(userID)
from log
group by userID)
select min(count),max(count),round(avg(count),2) from 
countByPage;""")
                for row in cursor:
                    print(row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def avgUserPerHour():
    try:
        with closing(psycopg2.connect(host="localhost", database="LogPython", password='postgres', user="postgres")) as conn:
            with conn.cursor() as cursor:
                cursor.execute("""with countPerHour as(Select DATE_PART('hour',timestamp),
                 count(DISTINCT userID) 
                from LOG
            group by DATE_PART('hour',timestamp)) 
            select round(avg(count),2) from countPerHour;""")
                for row in cursor:
                    print(row)

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def main(filepath):
    #createTable()
    #fillTable(filepath)
    #userPerMonth()
    #orderPerDay()
    #purchase()
    #minmaxavg()
    #avgUserPerHour()
    pass 
if __name__ == '__main__':
    main('access1.log')