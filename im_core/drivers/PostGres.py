from psycopg2.pool import ThreadedConnectionPool
from psycopg2.extras import DictCursor
import atexit


class PostGres:
    def __init__(self, db_host, db_port, db_database, db_username, db_password):
        self.connectionPool = ThreadedConnectionPool(
            5,  # Minimum connections in pool
            20,  # Maximum connections in pool
            host=db_host,
            port=db_port,
            user=db_username,
            password=db_password,
            database=db_database,
            cursor_factory=DictCursor
        )

        # Close all of the database connections on program exit
        atexit.register(self._close)

    def _close(self):
        self.connectionPool.closeall()

    def execute(self, query, values=None):
        conn = self.connectionPool.getconn()
        conn.autocommit = True

        try:
            with conn.cursor() as cursor:
                if "INSERT" in query:
                    cursor.execute(query, values)
                    return True
                elif "SELECT" in query:
                    cursor.execute(query, values)
                    rows = cursor.fetchall()
                    return rows
                elif "UPDATE" in query:
                    cursor.execute(query, values)
                    return True
                else:
                    print("Query is not allowed")
        finally:
            cursor.close()
            self.connectionPool.putconn(conn)
