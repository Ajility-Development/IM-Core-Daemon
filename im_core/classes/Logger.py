import inspect
from datetime import datetime
from colored import fg, attr
from im_core.classes.Singleton import Singleton

import im_core.classes


class Logger(metaclass=Singleton):
    def __init__(self, daemon_id):
        # Data
        self.db = im_core.classes.Database()
        self.store = im_core.classes.Store()

        self.daemon_id = daemon_id
        self.info = fg('51')
        self.warning = fg('3')
        self.danger = fg('9')
        self.success = fg('46')
        self.reset = attr('reset')

    def write(self, msg, level):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log = '[' + now + ']: <' + inspect.stack()[1][3] + '> ' + msg + self.reset

        self.store.conn.execute("""
            INSERT INTO logs (time, message, level, daemon_id) VALUES (?, ?, ?, ?)
        """, [now, msg, level, self.daemon_id])

        if level == 'info':
            print(self.info + log)
        elif level == 'warning':
            print(self.warning + log)
        elif level == 'danger':
            print(self.danger + log)
        elif level == 'success':
            print(self.success + log)
        else:
            print(self.info + log)

    def forward(self):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        records = self.store.conn.execute('SELECT time, message, level, daemon_id FROM logs WHERE time <= ?', [now]).fetchall()

        if len(records) > 0:
            listTrimmed = str(records)[1:-1].replace('"', '')
            query = (
                f'INSERT INTO logs '
                f'(time, message, level, daemon_id) '
                f'VALUES {listTrimmed} '
                f'ON CONFLICT DO NOTHING'
            )
            self.db.conn.execute(query)
            self.store.conn.execute('DELETE FROM logs WHERE time <= ?', [now])

