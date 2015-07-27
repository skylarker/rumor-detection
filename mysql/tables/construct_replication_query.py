import re


class ConstructReplicationQuery(object):
    def __init__(self):
        pass

    @staticmethod
    def construct_replication_query(conn, src_tbl, dst_tbl):
        """

        :param conn:
        :param src_tbl:
        :param dst_tbl:
        :return:
        """
        try:
            cursor = conn.cursor()
            cursor.execute("SHOW CREATE TABLE " + src_tbl)
            row = cursor.fetchone()
            cursor.close()
            if row is None:
                query = None
            else:
                # Replace src_tbl with dst_tbl in the CREATE TABLE statement
                query = re.sub("CREATE TABLE .*`" + src_tbl + "`",
                               "CREATE TABLE `" + dst_tbl + "`",
                               row[1])
        except:
            query = None
        return query