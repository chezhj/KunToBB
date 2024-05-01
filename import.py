"""Script to port kunena 5 to bbpress"""

# pylint: disable=missing-function-docstring
import configparser
from datetime import datetime
import logging

from mysql.connector import connect
import progressbar

from converters import ReplyConvertor, TopicConvertor


DRY_RUN = False


class MyDB(object):
    """Default database object to reuse connection and cursors"""

    def __init__(self, host, user, password, db):
        self._db_connection = connect(host=host, user=user, password=password, db=db)
        self._db_cur = self._db_connection.cursor(dictionary=True)
        self._db_insert_cur = self._db_connection.cursor()

    def query(self, query, params):
        return self._db_cur.execute(query, params)

    def execute(self, query, params):
        return self._db_insert_cur.execute(query, params)

    def execute_many(self, query, params):
        return self._db_insert_cur.executemany(query, params)

    def fetchall(self):
        return self._db_cur.fetchall()

    def get_lastid(self):
        return self._db_insert_cur.lastrowid

    def rollback(self):
        return self._db_connection.rollback()

    def commit(self):
        return self._db_connection.commit()

    def __del__(self):
        self._db_connection.close()


# menu_order=0
# post_type="topic"
# post_mime_type=""
# comment_count=0


def set_last_post_info(temp_dict, post_id, last_active_time):
    temp_dict["_bbp_last_reply_id"] = post_id
    temp_dict["_bbp_last_active_time"] = last_active_time


class KunenaData(object):
    """Class that handles the data we import/read from Kunena database"""

    def __init__(self, db, prefix) -> None:
        self.topic = None
        self.db = db
        self.topics = None
        self.replies = None
        self.topic_table = prefix + "_kunena_topics"
        self.mesages_table = prefix + "_kunena_messages"
        self.mesages_text_table = prefix + "_kunena_messages_text"

    def get_topics(self, category_id):
        select_topics_sql = f"""
        SELECT *
        FROM {self.topic_table}
        where category_id=%s
        """
        self.db.query(select_topics_sql, (category_id,))
        self.topics = self.db.fetchall()

    def get_replies(self, topic_id):
        select_reply_sql = f"""
        SELECT *, mtxt.message
        FROM {self.mesages_table}, 
        {self.mesages_text_table} as mtxt
        where thread=%s
        AND parent<>0
        AND mtxt.mesid=id 
        """
        self.db.query(select_reply_sql, (topic_id,))
        self.replies = self.db.fetchall()


class BbpressData(object):
    """Class that handles the export/insert into the new database"""

    def __init__(self, db, prefix) -> None:
        self.topic = None
        self.db = db
        self.topics = None
        self.posts_table = prefix + "_posts"
        self.post_meta_table = prefix + "_postmeta"

    def get_post_sql(self):
        return f"""
        INSERT INTO {self.posts_table}
        (post_author,post_date,post_date_gmt,post_content,post_title,post_status,
         comment_status,ping_status,post_name,post_modified,post_modified_gmt,
         post_parent,guid,menu_order,post_type,comment_count)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,
                %s,%s,%s,%s,%s,%s,%s,%s)
        """

    def get_meta_sql(self):
        return f"""
        INSERT INTO {self.post_meta_table}
        (post_id, meta_key, meta_value)
        VALUES (%s,%s,%s)
        """

    def insert_topic(self, c: TopicConvertor):

        value = (
            c.get_post_author(),
            c.get_post_date_fmt(),
            c.get_post_date_gmt_fmt(),
            c.get_post_content(),
            c.get_post_title(),
            c.get_post_status(),
            c.get_comment_status(),
            c.get_ping_status(),
            c.get_post_name(),
            c.get_post_modified(),
            c.get_post_modified_gmt(),
            c.get_post_parent(),
            c.get_guid(),
            c.get_menu_order(),
            c.get_post_type(),
            c.get_comment_count(),
        )
        self.db.execute(self.get_post_sql(), value)
        topic_id = self.db.get_lastid()

        meta_values = []
        meta_values.append((topic_id, "_bbp_forum_id", c.get_post_parent()))
        meta_values.append((topic_id, "_bbp_topic_id", topic_id))
        meta_values.append((topic_id, "_bbp_author_ip", "127.0.0.1"))
        meta_values.append((topic_id, "_bbp_reply_count", c.get_reply_count()))
        meta_values.append((topic_id, "_bbp_reply_count_hidden", 0))
        meta_values.append((topic_id, "_bbp_voice_count", 1))
        meta_values.append((topic_id, "_fusion", "a:0:{}"))
        meta_values.append((topic_id, "avada_post_views_count", c.get_hits()))
        meta_values.append((topic_id, "avada_today_post_views_count", 1))
        meta_values.append(
            (
                topic_id,
                "avada_post_views_count_today_date",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
        meta_values.append((topic_id, "kunena_topic_id", c.get_id()))
        meta_values.append((topic_id, "kunena_message_id", c.get_kun_first_post_id()))
        meta_values.append((topic_id, "kunena_user", c.get_username()))

        self.db.execute_many(self.get_meta_sql(), meta_values)

        return topic_id

    def insert_reply(self, r: ReplyConvertor, kun_bbp_ids):
        value = (
            r.get_post_author(),
            r.get_post_date_fmt(),
            r.get_post_date_gmt_fmt(),
            r.get_post_content(),
            r.get_post_title(),
            r.get_post_status(),
            r.get_comment_status(),
            r.get_ping_status(),
            "",
            r.get_post_modified(),
            r.get_post_modified_gmt(),
            r.get_post_parent(),
            "",
            r.get_menu_order(),
            r.get_post_type(),
            r.get_comment_count(),
        )
        self.db.execute(self.get_post_sql(), value)
        reply_id = self.db.get_lastid()
        kun_bbp_ids[r.get_id()] = reply_id

        update_sql = f"""
        UPDATE {self.posts_table}
        set post_name=%s, guid=%s 
        WHERE id=%s
        """
        self.db.execute(update_sql, (reply_id, r.get_guid(reply_id), reply_id))
        logging.debug("reply url %s", r.get_guid(reply_id))

        meta_values = []
        meta_values.append((reply_id, "_bbp_author_ip", "127.0.0.1"))
        meta_values.append((reply_id, "_bbp_forum_id", r.get_forum_id()))
        meta_values.append((reply_id, "_bbp_topic_id", r.get_post_parent()))

        if r.get_kun_parent() in kun_bbp_ids:
            meta_values.append(
                (reply_id, "_bbp_reply_to", kun_bbp_ids[r.get_kun_parent()])
            )
            logging.debug("reply parent found %d", kun_bbp_ids[r.get_kun_parent()])

        meta_values.append((reply_id, "_fusion", "a:0:{}"))
        meta_values.append((reply_id, "avada_post_views_count", 0))
        meta_values.append((reply_id, "avada_today_post_views_count", 0))
        meta_values.append(
            (
                reply_id,
                "avada_post_views_count_today_date",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
        meta_values.append((reply_id, "kunena_message_id", r.get_id()))
        meta_values.append((reply_id, "kunena_user", r.get_username()))

        self.db.execute_many(self.get_meta_sql(), meta_values)

        set_last_post_info(kun_bbp_ids, reply_id, r.get_post_date_fmt())
        logging.debug("last_post_date = %s", kun_bbp_ids["_bbp_last_active_time"])

    def add_topic_meta(self, topic_id, meta_dict):
        meta_values = []
        # _bbp_last_reply_id=0 bij geen reply, of id van laatste reply (kan pas aan het einde)
        # _bbp_last_active_id=_bbp_last_reply_id (kan pas aan het einde)
        # _bbp_last_active_time=op 2023 zetten? (last reply date, dan kan het pas aan het einde)
        meta_values.append(
            (topic_id, "_bbp_last_reply_id", meta_dict["_bbp_last_reply_id"])
        )
        meta_values.append(
            (topic_id, "_bbp_last_active_id", meta_dict["_bbp_last_reply_id"])
        )
        meta_values.append(
            (topic_id, "_bbp_last_active_time", meta_dict["_bbp_last_active_time"])
        )

        self.db.execute_many(self.get_meta_sql(), meta_values)


class ConvertController(object):
    """Controlling the conversion"""

    def __init__(self):
        self.config = configparser.ConfigParser()
        self.config.read("config.ini")
        self.kunena_db = None
        self.bbpress_db = None
        self.bbpress_data = None
        self.kunena_data = None

    def connect_datases(self):
        self.kunena_db = MyDB(
            self.config["sourceDB"]["host"],
            self.config["sourceDB"]["user"],
            self.config["sourceDB"]["pass"],
            self.config["sourceDB"]["db"],
        )
        self.bbpress_db = MyDB(
            self.config["targetDB"]["host"],
            self.config["targetDB"]["user"],
            self.config["targetDB"]["pass"],
            self.config["targetDB"]["db"],
        )

    def persist(self):
        if DRY_RUN:
            logging.info("Rolling back")
            self.bbpress_db.rollback()
        else:
            self.bbpress_db.commit()
            logging.info("Will commit in future")

    def load_topics(self, catid):
        self.kunena_data.get_topics(catid)

    def create_data_objects(self):
        self.kunena_data = KunenaData(
            self.kunena_db, self.config.get("sourceDB", "prefix")
        )
        self.bbpress_data = BbpressData(
            self.bbpress_db, self.config.get("targetDB", "prefix")
        )

    def start_conversion(self):
        self.connect_datases()
        self.create_data_objects()

        # Get Categories
        # need to change into query
        categories = [
            10,
        ]

        for cat in categories:
            print(f"Found {len(categories)} category(ies) to import")
            self.load_topics(cat)
            progress_bar = progressbar.ProgressBar(
                max_value=len(self.kunena_data.topics), redirect_stdout=True
            )

            for kun_topic in self.kunena_data.topics:
                kun_bbp_ids = {}
                converter = TopicConvertor(kun_topic, self.config)
                logging.debug(converter.get_post_date())
                logging.debug(converter.get_guid())
                self.kunena_data.get_replies(converter.get_id())

                bbp_topic_id = self.bbpress_data.insert_topic(converter)
                logging.debug("id of new inserted topic %d", bbp_topic_id)

                kun_bbp_ids[converter.get_kun_first_post_id] = bbp_topic_id
                set_last_post_info(kun_bbp_ids, 0, converter.get_post_date_fmt())
                logging.debug(
                    "last_post_date = %s", kun_bbp_ids["_bbp_last_active_time"]
                )

                for idx, kun_reply in enumerate(self.kunena_data.replies):
                    reply_conv = ReplyConvertor(
                        kun_reply,
                        self.config,
                        bbp_topic_id,
                        idx,
                        converter.get_post_parent(),
                    )
                    logging.debug(reply_conv.get_post_content())
                    self.bbpress_data.insert_reply(reply_conv, kun_bbp_ids)

                # add topic meta records on last reply
                self.bbpress_data.add_topic_meta(bbp_topic_id, kun_bbp_ids)
                self.persist()
                progress_bar.next()
            progress_bar.finish()


def import_controller():
    # Setup
    progressbar.streams.wrap_stderr()
    logging.basicConfig(
        format="%(asctime)s %(levelname)s - %(message)s",
        datefmt="%I:%M:%S",
        level=logging.DEBUG,
    )
    controller = ConvertController()

    # controller.verify
    controller.start_conversion()


if __name__ == "__main__":
    import_controller()
    # start_load()
