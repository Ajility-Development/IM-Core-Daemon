import sqlite3
import atexit
from pathlib import Path
from im_core.classes.Singleton import Singleton


class Store(metaclass=Singleton):
    def __init__(self):
        self.conn = sqlite3.connect(str(Path(__file__).parents[1]) + '/data/store.db')
        self.conn.isolation_level = None  # Auto Commit

        self._configure()

        # Close the database connection on program exit
        atexit.register(self.conn.close)

    def _configure(self):
        cur = self.conn.cursor()

        try:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS facts (
                    tag_id INT NOT NULL,
                    time TIMESTAMPTZ NOT NULL,
                    val DOUBLE PRECISION NOT NULL
                )
            """)
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS facts_tag_id_time_idx ON facts (tag_id, time)")

            cur.execute("""
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    time TIMESTAMPTZ NOT NULL,
                    message TEXT NOT NULL,
                    level TEXT NOT NULL,
                    daemon_id INT
                )
            """)
        finally:
            self.conn.commit()

    def execute(self, query, values=None):
        cur = self.conn.cursor()

        try:
            if "CREATE" in query:
                cur.execute(query, values)
                return True
            elif "INSERT" in query:
                cur.execute(query, values)
                return True
            elif "SELECT" in query:
                cur.execute(query, values)
                rows = cur.fetchall()
                return rows
            elif "UPDATE" in query:
                cur.execute(query, values)
                return True
            else:
                print("Query is not allowed")
        finally:
            cur.close()
