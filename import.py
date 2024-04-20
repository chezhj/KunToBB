"""Script to port kunena 5 to bbpress"""

import datetime
from dateutil import tz
from mysql.connector import connect, Error
from slugify import slugify

##Some setup needs to come here

try:
    connection = connect(
        host="87.236.98.81", password="2548tk", user="picknl", database="picknl"
    )
    print(connection)
except Error as e:
    print(e)
    exit()


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
