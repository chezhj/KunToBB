"""Script to port kunena 5 to bbpress"""

import configparser
import datetime
import sys

from dateutil import tz
from mysql.connector import Error, connect
from slugify import slugify

##Some setup needs to come here


##import MySQLdb.cursors


class TopicConvertor:
    """Converts kunena topic row result from query into field needed for bbpress"""

    def __init__(self, query_row):
        self.row = query_row

    def get_post_author(self):
        return 8  # dummy user to be created

    def get_post_date(self):
        return datetime.datetime.fromtimestamp(self.row["first_post_time"])

    def get_post_date_gmt(self):
        date = self.get_post_date()
        return date.astimezone(tz.tzutc())

    def get_post_content(self):
        return self.row["first_post_message"]

    def get_post_title(self) -> str:
        return self.row["subject"]

    def get_post_status(self):
        return "publish"

    def get_comment_status(self):
        return "closed"

    def get_ping_status(self):
        return "closed"

    def get_post_name(self) -> str:
        return slugify(self.get_post_title() + "-" + str(self.row["first_post_id"]))

    def get_post_modified(self):
        return self.get_post_date()

    def get_post_modified_gmt(self):
        return self.get_post_date_gmt

    # def get_post_parent(self,)
    # post_parent=FORUM ID!

    def get_guid(self, base):
        # https://staging.ftdlotgenoten.nl/forums/topic/test-threads-2/
        return base + self.get_post_name()


# menu_order=0
# post_type="topic"
# post_mime_type=""
# comment_count=0


def start_load():
    config = configparser.ConfigParser()
    config.read("config.ini")

    try:
        connection = connect(
            host=config["sourceDB"]["host"],
            password=config["sourceDB"]["pass"],
            user=config["sourceDB"]["user"],
            database=config["sourceDB"]["db"],
        )

    except Error as e:
        print(e)
        sys.exit()

    select_query = """
    SELECT *
    FROM nft_kunena_topics
    where category_id=11
    """

    cursor = connection.cursor(dictionary=True)

    cursor.execute(select_query)
    for kun_topic in cursor.fetchall():
        converter = TopicConvertor(kun_topic)
        print(converter.get_post_date())
        print(converter.get_post_date_gmt())
        print(converter.get_guid(config["settings"]["baseURL"]))
    # print(slugify(topic[1]))

    connection.close()


if __name__ == "__main__":
    start_load()
