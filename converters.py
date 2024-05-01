# pylint: disable=missing-function-docstring
import datetime

from dateutil import tz
from slugify import slugify


class TopicConvertor:
    """Converts kunena topic row result from query into field needed for bbpress"""

    def __init__(self, query_row, conf):
        self.row = query_row
        self.conf = conf

    def get_post_author(self):
        return self.conf.getint("settings", "import_user_id")

    def get_post_date(self):
        return datetime.datetime.fromtimestamp(self.row["first_post_time"])

    def get_post_date_fmt(self):
        return self.get_post_date().strftime("%Y-%m-%d %H:%M:%S")

    def get_post_date_gmt_fmt(self):
        date = self.get_post_date()
        return date.astimezone(tz.tzutc()).strftime("%Y-%m-%d %H:%M:%S")

    def get_post_content(self):
        return self.row["first_post_message"]

    def get_post_title(self) -> str:
        return self.row["subject"]

    def get_post_status(self):
        return "publish"

    def get_kun_first_post_id(self):
        return self.row["first_post_id"]

    def get_comment_status(self):
        return "closed"

    def get_ping_status(self):
        return "closed"

    def get_post_name(self) -> str:
        return slugify(self.get_post_title() + "-" + str(self.row["first_post_id"]))

    def get_post_modified(self):
        return self.get_post_date_fmt()

    def get_post_modified_gmt(self):
        return self.get_post_date_gmt_fmt()

    def get_category(self):
        return self.row["category_id"]

    def get_reply_count(self):
        return self.row["posts"] - 1

    def get_post_parent(self):
        forum_id = self.conf.getint("category", str(self.get_category()))
        # raise error or fallback??
        return forum_id

    def get_post_type(self):
        return "topic"

    def get_id(self):
        return self.row["id"]

    def get_menu_order(self):
        return 0

    def get_comment_count(self):
        return 0

    def get_hits(self):
        return self.row["hits"]

    def get_username(self):
        return self.row["first_post_guest_name"]

    def get_guid(self):
        base = self.conf.get("settings", "baseURL")
        if not (base.endswith("/")):
            base = base + "/"
        return base + "topic/" + self.get_post_name()


class ReplyConvertor:
    """Converts kunena reply row result from query into field needed for bbpress"""

    def __init__(self, query_row, conf, post_parent_id, reply_index, forum_id):
        self.row = query_row
        self.conf = conf
        self.post_parent = post_parent_id
        self.reply_index = reply_index
        self.forum_id = forum_id

    def get_post_author(self):
        return self.conf.getint("settings", "import_user_id")

    def get_post_date(self):
        return datetime.datetime.fromtimestamp(self.row["time"])

    def get_post_date_fmt(self):
        return self.get_post_date().strftime("%Y-%m-%d %H:%M:%S")

    def get_post_date_gmt_fmt(self):
        date = self.get_post_date()
        return date.astimezone(tz.tzutc()).strftime("%Y-%m-%d %H:%M:%S")

    def get_post_content(self):
        return self.row["message"]

    def get_post_title(self):
        return self.row["subject"]

    def get_post_status(self):
        return "publish"

    def get_kun_parent(self):
        return self.row["parent"]

    def get_comment_status(self):
        return "closed"

    # post_name=reply-id

    def get_forum_id(self):
        return self.forum_id

    def get_ping_status(self):
        return "closed"

    def get_post_modified(self):
        return self.get_post_date_fmt()

    def get_post_modified_gmt(self):
        return self.get_post_date_gmt_fmt()

    # post_parent=TOPIC.id
    def get_post_parent(self):
        return self.post_parent

    def get_id(self):
        return self.row["id"]

    # guid=url/forums/topic/post_name
    # moet achteraf, want postname=id

    # menu_order=is reply order 1,2,3
    def get_menu_order(self):
        return self.reply_index

    def get_username(self):
        return self.row["name"]

    def get_post_type(self):
        return "reply"

    def get_comment_count(self):
        return 0

    def get_guid(self, new_name):
        base = self.conf.get("settings", "baseURL")
        if not (base.endswith("/")):
            base = base + "/"
        return base + "reply/" + str(new_name)
