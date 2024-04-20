"""Script to port kunena 5 to bbpress"""

import configparser
import datetime
import sys

from dateutil import tz
from mysql.connector import Error, connect
from slugify import slugify

##Some setup needs to come here


##import MySQLdb.cursors

config = configparser.ConfigParser()
config.read("config.ini")


##def connect():
##    return MySQLdb.connect(host = config['mysqlDB']['host'],
##                           user = config['mysqlDB']['user'],
##                           passwd = config['mysqlDB']['pass'],
##                           db = config['mysqlDB']['db'])

try:
    ##connection = connect(
    ##    host="87.236.98.81", password="2548tk", user="picknl", database="picknl"
    ##)
    connection = connect(
        host=config["sourceDB"]["host"],
        password=config["sourceDB"]["pass"],
        user=config["sourceDB"]["user"],
        database=config["sourceDB"]["db"],
    )

    print(connection)
except Error as e:
    print(e)
    sys.exit()


SELECT_QUERY = """
    SELECT id, subject, first_post_id, first_post_time
    FROM nft_kunena_topics
    where category_id=11
    """
with connection.cursor() as cursor:
    cursor.execute(SELECT_QUERY)
    for topic in cursor.fetchall():
        print(topic)
        print(topic[3])
        date = datetime.datetime.fromtimestamp(topic[3])
        print(date)
        date = date.replace(tzinfo=tz.tzlocal())
        utc_time = date.astimezone(tz.tzutc())
        print(utc_time)
        print(slugify(topic[1]))

connection.close()
