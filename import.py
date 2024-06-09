"""Script to port kunena 5 to bbpress"""

# pylint: disable=missing-function-docstring
import configparser
from datetime import datetime
import logging

import mysql.connector


import progressbar

from converters import CategoryConvertor, ReplyConvertor, TopicConvertor

# Set this to False if you are sure and want to commit!
DRY_RUN = True


class MyDB:
    """Default database object to reuse connection and cursors"""

    def __init__(self, host, user, password, db):
        try:
            self._db_connection = mysql.connector.connect(
                host=host, user=user, password=password, db=db
            )
            self._db_cur = self._db_connection.cursor(dictionary=True)
            self._db_insert_cur = self._db_connection.cursor()
        except mysql.connector.Error as e:
            logging.error(e)
            self._db_connection = None
            self._db_cur = None
            self._db_insert_cur = None

    def connected(self):
        if self._db_connection:
            self.query("SELECT VERSION()", None)
            result = self.fetchall()
            return result is not None

        return False

    def query(self, query, params):
        return self._db_cur.execute(query, params)

    def execute(self, query, params):
        return self._db_insert_cur.execute(query, params)

    def execute_many(self, query, params):
        return self._db_insert_cur.executemany(query, params)

    def fetchall(self):
        return self._db_cur.fetchall()

    def fetchone(self):
        return self._db_cur.fetchone()

    def get_lastid(self):
        return self._db_insert_cur.lastrowid

    def rollback(self):
        return self._db_connection.rollback()

    def commit(self):
        return self._db_connection.commit()

    def __del__(self):
        if self._db_connection:
            self._db_connection.close()


# menu_order=0
# post_type="topic"
# post_mime_type=""
# comment_count=0


def set_last_post_info(temp_dict, post_id, last_active_time):
    temp_dict["_bbp_last_reply_id"] = post_id
    temp_dict["_bbp_last_active_time"] = last_active_time
    # need to save topic_id to fill forum meta at the end


class KunenaData:
    """Class that handles the data we import/read from Kunena database"""

    # pylint: disable=too-many-instance-attributes
    def __init__(self, db, prefix) -> None:
        self.topic = None
        self.db = db
        self.topics = None
        self.replies = None
        self.categories = None
        self.topic_table = prefix + "_kunena_topics"
        self.mesages_table = prefix + "_kunena_messages"
        self.mesages_text_table = prefix + "_kunena_messages_text"
        self.categories_table = prefix + "_kunena_categories"

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

    def get_categories(self, parent_id):
        select_category_sql = f"""
        SELECT * 
        FROM {self.categories_table}
        where parent_id={parent_id}
        and locked <> 1 
        order by ordering
        """
        self.db.query(select_category_sql, None)
        self.categories = self.db.fetchall()

    def check_tables(self):
        # select tables and check if the tables exist
        # SELECT * FROM information_schema.tables
        # WHERE table_name = self.topic_table
        # WHERE table_name = self.mesages_table
        # WHERE table_name = self.mesages_text_table
        # WHERE table_name = self.categories_table
        check_table_sql = f"""
        select * FROM information_schema.tables
        where table_name IN ('{self.topic_table}', '{self.mesages_table}', '{self.mesages_text_table}', '{self.categories_table}')
        """
        self.db.query(check_table_sql, None)
        return self.db.fetchall()

    def get_nof_categories(self, parent_id):
        select_category_sql = f"""
        SELECT count(*) as nof_categories
        FROM {self.categories_table}
        where parent_id={parent_id}
        and locked <> 1 
        """
        self.db.query(select_category_sql, None)
        return self.db.fetchone()

    def get_nof_topics(self, parent_id):
        select_topic_sql = f"""
        SELECT categorie.name, count(topic.id) as nof_topics
        FROM {self.topic_table} as topic,
        {self.categories_table} as categorie
        where categorie.id=topic.category_id
        and categorie.parent_id={parent_id}
        and categorie.locked <> 1
        group by categorie.name
        """
        self.db.query(select_topic_sql, None)
        return self.db.fetchall()


class BbpressData:
    """Class that handles the export/insert into the new database"""

    def __init__(self, db, prefix) -> None:
        self.topic = None
        self.db = db
        self.topics = None
        self.posts_table = prefix + "_posts"
        self.post_meta_table = prefix + "_postmeta"
        self.users_table = prefix + "_users"

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

    def insert_forum(self, c: CategoryConvertor):

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
            "",
            c.get_menu_order(),
            c.get_post_type(),
            c.get_comment_count(),
        )
        self.db.execute(self.get_post_sql(), value)

        forum_id = self.db.get_lastid()

        update_sql = f"""
        UPDATE {self.posts_table}
        set guid=%s 
        WHERE id=%s
        """
        self.db.execute(update_sql, (c.get_guid(forum_id), forum_id))
        logging.debug("reply url %s", c.get_guid(forum_id))

        meta_values = []
        meta_values.append((forum_id, "_edit_lock", ""))
        meta_values.append((forum_id, "_edit_last", c.get_post_date_fmt()))
        meta_values.append((forum_id, "_bbp_forum_subforum_count", 0))
        meta_values.append((forum_id, "_fusion", c.get_fusion_meta()))

        meta_values.append((forum_id, "_bbp_total_reply_count_hidden", 0))
        meta_values.append((forum_id, "_bbp_status", "open"))
        meta_values.append((forum_id, "_bbp_forum_type", "forum"))
        meta_values.append((forum_id, "_yoast_wpseo_estimated-reading-time-minutes", 0))
        meta_values.append((forum_id, "_yoast_wpseo_wordproof_timestamp", ""))
        meta_values.append((forum_id, "avada_post_views_count", 1))
        meta_values.append((forum_id, "avada_today_post_views_count", 1))
        meta_values.append(
            (
                forum_id,
                "avada_post_views_count_today_date",
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            )
        )
        meta_values.append((forum_id, "_bbp_forum_id", c.get_post_parent()))

        meta_values.append((forum_id, "_bbp_topic_count", 0))
        meta_values.append((forum_id, "_bbp_total_topic_count", 0))
        meta_values.append((forum_id, "_bbp_topic_count_hidden", 0))
        meta_values.append((forum_id, "_bbp_reply_count", 0))
        meta_values.append((forum_id, "_bbp_total_reply_count", 0))

        self.db.execute_many(self.get_meta_sql(), meta_values)

        return forum_id

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
        return reply_id

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

    def add_forum_meta(self, forum_id, meta_dict):

        meta_values = []
        meta_values.append(
            (forum_id, "_bbp_last_topic_id", meta_dict["conversion_last_topic_id"])
        )
        meta_values.append(
            (forum_id, "_bbp_last_reply_id", meta_dict["_bbp_last_reply_id"])
        )
        meta_values.append(
            (forum_id, "_bbp_last_active_id", meta_dict["_bbp_last_reply_id"])
        )
        meta_values.append(
            (forum_id, "_bbp_last_active_time", meta_dict["_bbp_last_active_time"])
        )
        self.db.execute_many(self.get_meta_sql(), meta_values)

    def check_tables(self):
        check_table_sql = f"""
        select * FROM information_schema.tables
        where table_name IN ('{self.post_meta_table}', '{self.posts_table}')
        """
        self.db.query(check_table_sql, None)
        return self.db.fetchall()

    def get_forum(self, forum_id):
        forum_sql = f"""
        SELECT id,post_title 
        FROM {self.posts_table}
        WHERE id=%s
        and post_type='forum'
        """
        self.db.query(forum_sql, (forum_id,))
        return self.db.fetchone()

    def check_users(self, import_user_id, admin_user_id):
        check_users_sql = f"""
        SELECT id, user_nicename 
        FROM {self.users_table}
        WHERE id=%s or id=%s    
        """
        self.db.query(check_users_sql, (import_user_id, admin_user_id))
        return self.db.fetchall()


class ConvertController:
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
            # logging.info("Will commit in future")

    def load_topics(self, catid):
        self.kunena_data.get_topics(catid)

    def create_data_objects(self):
        self.kunena_data = KunenaData(
            self.kunena_db, self.config.get("sourceDB", "prefix")
        )
        self.bbpress_data = BbpressData(
            self.bbpress_db, self.config.get("targetDB", "prefix")
        )

    def ask_yes_no_question(self):
        response = input("Do you want to start the migration? (yes/no) ")
        while response.lower() not in ["yes", "no"]:
            print("Please enter 'yes' or 'no'.")
            response = input("Do you want to start the migration? (yes/no) ")
        return response.lower() == "yes"

    def verify(self):
        # Check source connection
        self.connect_datases()
        if not self.kunena_db.connected:
            print("Source databases not connected, exiting")
            return False
        print("Kunena database connected")

        # check target connections
        if not self.bbpress_db.connected:
            print("Target (bbpress) database not connected, exiting")
            return False
        print("Target (bbpress) database connected")

        self.create_data_objects()

        # check source table
        tables = self.kunena_data.check_tables()
        if len(tables) == 0:
            print("Source tables not found, wrong SourceDB prefix? Exiting....")
            return False
        print("Source tables found")

        # check target tables
        tables = self.bbpress_data.check_tables()
        if len(tables) == 0:
            print("Target tables not found, wrong TargetDB prefix? Exiting....")
            return False
        print("Target tables found")

        # check nof categories for parent  id
        parent_id = self.config.getint("category", "parent_id")
        nof_categories = self.kunena_data.get_nof_categories(parent_id)
        if nof_categories["nof_categories"] == 0:
            print("No categories found for parent id, exiting...")
            return False
        print(
            f"Found {nof_categories['nof_categories']} categorie(s) for parent id {parent_id}"
        )

        # check nof topics
        topiccount = self.kunena_data.get_nof_topics(parent_id)
        for category in topiccount:
            print(
                f"Found {category['nof_topics']} topic(s) in category \"{category['name']}\""
            )

        # check parent forum
        forum = self.bbpress_data.get_forum(
            self.config.getint("category", "main_forum")
        )
        if forum is None:
            print("Parent forum not found, exiting...")
            return False
        print(
            f"Parent forum found with id \"{forum['id']}\" and name \"{forum['post_title']}\""
        )

        # check users exits
        import_user = self.config.getint("settings", "import_user_id")
        admin_user = self.config.getint("settings", "admin_user_id")
        users = self.bbpress_data.check_users(import_user, admin_user)
        for user in users:
            user_txt = "import user"
            if user["id"] == admin_user:
                user_txt = "ADMIN USER"
            print(
                f'Found {user_txt} with id "{user["id"]}" and name "{user["user_nicename"]}"'
            )

        if len(users) != 2:
            print("Import user and admin user not found, exiting...")
            return False
        print("Import user and admin user found")

        # check if script runs in DRY_RUN mode
        if DRY_RUN:
            print("Running in DRY_RUN mode, changes will be rolled back.")
            print(
                "Note that numbers used for auto increment fields (id's) will be consumed."
            )
        else:
            print("Running in LIVE mode, changes will be committed.")

        # Get conformation,user inputs yes, from commandline then continu
        return self.ask_yes_no_question()

    def start_conversion(self):
        self.connect_datases()
        self.create_data_objects()

        # Get Categories
        # need to change into query
        self.kunena_data.get_categories(self.config.getint("category", "parent_id"))
        print(f"Found {len(self.kunena_data.categories)} category(ies) to import")
        for cat in self.kunena_data.categories:
            print(f"Starting import of {cat['name']}")
            converted_categeory = CategoryConvertor(cat, self.config)

            forum_id = self.bbpress_data.insert_forum(converted_categeory)

            self.load_topics(converted_categeory.get_id())

            progress_bar = progressbar.ProgressBar(
                max_value=len(self.kunena_data.topics), redirect_stdout=True
            )

            for kun_topic in self.kunena_data.topics:
                kun_bbp_ids = {}
                converter = TopicConvertor(kun_topic, self.config, forum_id)
                logging.debug(converter.get_post_date())
                logging.debug(converter.get_guid())
                self.kunena_data.get_replies(converter.get_id())

                bbp_topic_id = self.bbpress_data.insert_topic(converter)
                logging.debug("id of new inserted topic %d", bbp_topic_id)

                kun_bbp_ids[converter.get_kun_first_post_id()] = bbp_topic_id
                set_last_post_info(kun_bbp_ids, 0, converter.get_post_date_fmt())
                kun_bbp_ids["conversion_last_topic_id"] = bbp_topic_id

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
            self.bbpress_data.add_forum_meta(forum_id, kun_bbp_ids)


def import_controller():
    # Setup
    progressbar.streams.wrap_stderr()
    logging.basicConfig(
        format="%(asctime)s %(levelname)s - %(message)s",
        datefmt="%I:%M:%S",
        level=logging.INFO,
    )
    controller = ConvertController()

    if controller.verify():
        controller.start_conversion()


if __name__ == "__main__":
    import_controller()
