"""Script to port kunena 5 to bbpress"""

import configparser
import datetime
import sys

from dateutil import tz
from mysql.connector import Error, connect
from slugify import slugify

##Some setup needs to come here


##import MySQLdb.cursors


class MyDB(object):

    def __init__(self, host, user, password, db):
        self._db_connection = connect(host=host, user=user, password=password, db=db)
        self._db_cur = self._db_connection.cursor(dictionary=True)

    def query(self, query, params):

        return self._db_cur.execute(query, params)

    def fetchall(self):
        return self._db_cur.fetchall()

    def __del__(self):
        self._db_connection.close()


class TopicConvertor:
    """Converts kunena topic row result from query into field needed for bbpress"""

    def __init__(self, query_row, conf):
        self.row = query_row
        self.conf = conf

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

    def get_category(self):
        return self.row["category_id"]

    def get_post_parent(self):
        forum_id = self.conf.getint("category", str(self.get_category()), fallback=0)
        # raise error or fallback??
        return forum_id

    def get_post_type(self):
        return "topic"

    def get_id(self):
        return self.row["id"]

    # post_parent=FORUM ID!

    def get_guid(self):
        base = self.conf.get("settings", "baseURL")
        if not (base.endswith("/")):
            base = base + "/"
        return base + self.get_post_name()


# menu_order=0
# post_type="topic"
# post_mime_type=""
# comment_count=0


def start_load():
    config = configparser.ConfigParser()
    config.read("config.ini")

    kunena_db = MyDB(
        config["sourceDB"]["host"],
        config["sourceDB"]["user"],
        config["sourceDB"]["pass"],
        config["sourceDB"]["db"],
    )

    kun_prefix = config.get("sourceDB", "prefix")
    kun_topic_table = kun_prefix + "_kunena_topics"
    kun_mesages_table = kun_prefix + "_kunena_messages"
    kun_mesages_text_table = kun_prefix + "_kunena_messages_text"

    select_topics_sql = f"""
    SELECT *
    FROM {kun_topic_table}
    where category_id=8
    """

    select_reply_sql = f"""
    SELECT * 
    FROM {kun_mesages_table}
    where thread=%s
    AND parent<>0 
    """

    kunena_db.query(select_topics_sql, None)
    for kun_topic in kunena_db.fetchall():
        converter = TopicConvertor(kun_topic, config)
        print(converter.get_post_date())
        print(converter.get_post_date_gmt())
        print(converter.get_guid())
        print(converter.get_category())
        # forum_id = int(config["categoonverter.get_category()ry"][str(converter.get_category())])
        print(f"Forum id =  {converter.get_post_parent()}")
        print(converter.get_id())
        kunena_db.query(select_reply_sql, (converter.get_id(),))

        for kun_reply in kunena_db.fetchall():
            print(kun_reply["subject"])


def import_controller():
    # Setup
    # Get Categories
    # report progress
    # Load Topics for each category
    # load Replies for topic
    # Insert Topic
    ##   insert replies
    ##   insert meta_reply
    # insert meta_topic
    start_load()


# bar = Bar('Processing', max=20)
# for i in range(20):
#     # Do some work
#     bar.next()
# bar.finish()


if __name__ == "__main__":
    import_controller()
