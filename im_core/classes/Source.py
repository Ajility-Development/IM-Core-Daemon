import time
from collections import OrderedDict
from datetime import datetime
from psycopg2 import OperationalError
import im_core.classes.Database
import im_core.classes.Store
import im_core.drivers


class Source:
    def __init__(self, sid):
        # Settings
        self.id = sid
        self.active = False
        self._address = None
        self._driver = None
        self.driver_instance = None
        self.last_heartbeat = None
        self.logger = im_core.classes.Logger()
        self.logger.write('Initializing source with id ' + str(self.id) + '...', 'info')

        self.db = im_core.classes.Database()
        self.store = im_core.classes.Store()

        # Setup
        self._configure()
        self._upsertDiscoveredTags()
        self._getMonitoringTags()
        self.logger.write('Source with id ' + str(self.id) + ' is initialized...', 'success')

    def _configure(self):
        try:
            source = self.db.conn.execute(
                "SELECT active, address, driver FROM sources WHERE id = %s",
                [self.id]
            )[0]
            self.logger.write('Configuring driver for source with id ' + str(self.id) + '...', 'info')

            if source:
                self.active = source['active']
                self._address = source['address']
                self._driver = source['driver']

                if self._driver == 'Logix':
                    self.driver_instance = im_core.drivers.Logix(self.id, self._address)
                    self._heartBeat()
                else:
                    self.logger.write('Invalid source driver specified for source with id ' + str(self.id) + '...', 'danger')
        except OperationalError:
            self.logger.write(
                'Communication error with the cloud while configuring source with id ' + str(self.id) + '...',
                'danger'
            )
            time.sleep(5)
            self._configure()

    def _heartBeat(self):
        try:
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

            if self.db.conn.execute(
                "UPDATE sources SET last_communication = %s WHERE id = %s",
                [now, self.id]
            ):
                self.last_heartbeat = now
        except OperationalError:
            self.logger.write(
                'Communication error with the cloud while recording heartbeat for source with id ' + str(self.id) + '...',
                'warning'
            )

    def _upsertDiscoveredTags(self):
        try:
            self.logger.write('Syncing tags for source with id ' + str(self.id) + ' with cloud...', 'info')
            now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            tags = []

            for key, val in self.driver_instance.discoveredTags.items():
                tags.append((key, val, self.id, now))

            listTrimmed = str(tags)[1:-1]
            query = (
                f'INSERT INTO tags '
                f'(name, data_type_name, source_id, created_at) '
                f'VALUES {listTrimmed} '
                f'ON CONFLICT (name, source_id) DO UPDATE SET '
                f'data_type_name = EXCLUDED.data_type_name, '
                f'updated_at = EXCLUDED.created_at;'
            )

            self.db.conn.execute(query)
            self.logger.write('Tags in cloud synced for source with id ' + str(self.id) + '...', 'success')
        except OperationalError:
            self.logger.write(
                'Communication error with the cloud while syncing tags for source with id ' + str(self.id) + '...',
                'warning'
            )

    def _getMonitoringTags(self):
        try:
            mTags = self.db.conn.execute(
                "SELECT id, name FROM tags WHERE monitor = true"
            )

            for tag in mTags:
                self.driver_instance.monitoringTags[tag['name']] = im_core.classes.Tag(tag['id'], tag['name'])
        except OperationalError:
            self.logger.write(
                'Communication error with the cloud while getting monitored tag list for source with id ' + str(self.id) + '... trying again in 5 seconds',
                'danger'
            )
            time.sleep(5)
            self._getMonitoringTags()

    def sync(self):
        try:
            source = self.db.conn.execute(
                "SELECT active FROM sources WHERE id = %s", [self.id]
            )[0]

            if source:
                self.active = source['active']
                self._heartBeat()

            if self.active:
                mtags = self.db.conn.execute("SELECT id, name FROM tags WHERE monitor = true")
                mTagsName = [name for sublist in self.db.conn.execute("SELECT name FROM tags WHERE monitor = true") for
                             name
                             in
                             sublist]
                tagsToRemove = [name for name in self.driver_instance.monitoringTags.keys() if name not in mTagsName]
                tagsToAdd = [name for name in mTagsName if name not in self.driver_instance.monitoringTags.keys()]

                for tag in tagsToRemove:
                    del self.driver_instance.monitoringTags[tag]

                for tag in mtags:
                    if tag['name'] in tagsToAdd:
                        self.driver_instance.monitoringTags[tag['name']] = im_core.classes.Tag(tag['id'], tag['name'])

                self.logger.write(str(len(self.driver_instance.monitoringTags)) + ' tags being monitored on source with id ' + str(self.id) + '...', 'info')
            else:
                self.driver_instance.monitoringTags = OrderedDict()

            self.logger.write('Settings for source with id ' + str(self.id) + ' synced with cloud...', 'success')

        except OperationalError:
            self.logger.write(
                'Communication error with the cloud while syncing settings for source with id ' + str(self.id) + '...',
                'danger'
            )

    def discoverTags(self):
        self.driver_instance.discoverTags()
        self._upsertDiscoveredTags()

    def poll(self):
        self.driver_instance.poll()

    def storeData(self):
        start = time.time()
        allRecords = []

        for tag in self.driver_instance.monitoringTags.values():
            if len(tag.records) > 0:
                allRecords.append(str(tag.records)[1:-1])
                tag.records = []

        if len(allRecords) > 0:
            listTrimmed = str(allRecords)[1:-1].replace('"', '')
            query = (
                f'INSERT OR IGNORE INTO facts '
                f'(tag_id, time, val) '
                f'VALUES {listTrimmed}'
            )

            self.store.conn.execute(query)
            self.logger.write('Wrote data to store for source with id ' + str(self.id) + ' (' + str(round(time.time() - start, 2)) + 's)...', 'success')
