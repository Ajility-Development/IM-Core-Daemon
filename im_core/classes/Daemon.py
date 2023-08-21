import os
from pathlib import Path
from collections import OrderedDict
from dotenv import load_dotenv
from datetime import datetime
import time
from psycopg2 import OperationalError
import im_core.classes

env_path = str(Path(__file__).parents[1]) + '/.env'
load_dotenv(dotenv_path=env_path)


class Daemon:
    def __init__(self):

        # Settings
        self.id = None
        self.active = False
        self._configKey = os.environ.get('CONFIGURATION_KEY')
        self.last_heartbeat = None
        self.poll_time = float(os.environ.get('POLL_TIME'))
        self.store_time = float(os.environ.get('STORE_TIME'))
        self.sync_time = float(os.environ.get('SYNC_TIME'))
        self.forward_time = float(os.environ.get('FORWARD_TIME'))

        # Data
        self.db = im_core.classes.Database()
        self.logger = self._logger()
        self.store = im_core.classes.Store()
        self.sources = OrderedDict()

        # Setup
        self.logger.write('Initializing the daemon...', 'info')
        self._configure()
        self.logger.write('Daemon is initialized...', 'success')

        # Helper variables
        self._pausedCounter = 0

    def _logger(self):
        try:
            daemon = self.db.conn.execute(
                    "SELECT id FROM daemons WHERE config_key = %s",
                    [self._configKey]
                )[0]

            if daemon:
                return im_core.classes.Logger(daemon['id'])
            else:
                print('Error configuring logger... does a daemon with this configuration key exist? Trying again in 5 seconds...')
                time.sleep(5)
                self._logger()
        except Exception as e:
            print('Error configuring logger... has the cloud database been initialized, and does a daemon with this configuration key exist? Trying again in 5 seconds...')
            print(e)
            time.sleep(5)
            self._logger()


    def _configure(self):
        try:
            # Configure Daemon
            daemon = self.db.conn.execute(
                "SELECT id, active FROM daemons WHERE config_key = %s",
                [self._configKey]
            )[0]

            if daemon:
                self.id = daemon['id']
                self.active = daemon['active']
                self._heartBeat()

                # Configure Sources
                sources = self.db.conn.execute(
                    "SELECT id FROM sources WHERE daemon_id = %s",
                    [self.id]
                )

                if sources:
                    for src in sources:
                        self.sources[src['id']] = im_core.classes.Source(src['id'])
        except OperationalError:
            self.logger.write('Communication error with the cloud while configuring the daemon... trying again in 5 seconds', 'danger')
            time.sleep(5)
            self._configure()

    def _heartBeat(self):
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if self.db.conn.execute(
                "UPDATE daemons SET last_communication = %s WHERE id = %s",
                [now, self.id]
            ):
                self.last_heartbeat = now
        except OperationalError:
            self.logger.write('Communication error with the cloud while recording daemon heartbeat', 'warning')

    def discoverSourceTags(self):
        for source in self.sources.values():
            if source.active:
                source.discoverTags()

    def syncDaemon(self):
        try:
            daemon = self.db.conn.execute(
                "SELECT active FROM daemons WHERE id = %s", [self.id]
            )[0]

            if daemon:
                self.active = daemon['active']
                self._heartBeat()

            if not self.active:
                self.logger.write('Daemon is in a paused state. Operations are suspended...', 'warning')

        except OperationalError:
            self.logger.write('Communication error with the cloud while syncing daemon settings', 'warning')

    def syncSources(self):
        if self.active:
            for source in self.sources.values():
                source.sync()

    def pollSources(self):
        if self.active:
            for source in self.sources.values():
                if source.active:
                    source.poll()
                    self._pausedCounter = 0
                elif not source.active:
                    if self._pausedCounter == 0:
                        self.logger.write('Source with id ' + str(source.id) + ' is in a paused state. Data collection is suspended...', 'warning')
                        self._pausedCounter += 1
                    elif self._pausedCounter >= 30:
                        self._pausedCounter = 0
                    else:
                        self._pausedCounter += 1


    def storeData(self):
        for source in self.sources.values():
            if self.active:
                source.storeData()

    def forwardData(self):
        start = time.time()
        if self.active:
            try:
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                records = self.store.conn.execute('SELECT tag_id, time, val FROM facts WHERE time <= ?', [now]).fetchall()

                if len(records) > 0:
                    listTrimmed = str(records)[1:-1].replace('"', '')
                    query = (
                        f'INSERT INTO facts '
                        f'(tag_id, time, val) '
                        f'VALUES {listTrimmed} '
                        f'ON CONFLICT (tag_id, time) DO NOTHING'
                    )
                    self.db.conn.execute(query)
                    self.store.conn.execute('DELETE FROM facts WHERE time <= ?', [now])
                    self.logger.write('Forwarded data to cloud (' + str(round(time.time() - start, 2)) + 's)...', 'success')
            except OperationalError:
                self.logger.write('Failed to forward tag data to cloud... continuing to store locally', 'danger')

    def forwardLogs(self):
        try:
            self.logger.forward()
        except OperationalError:
            self.logger.write('Failed to forward logs to cloud... continuing to store locally', 'danger')
