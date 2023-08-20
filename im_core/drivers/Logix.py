import re
from collections import OrderedDict
from datetime import datetime
import time
import atexit
from pycomm3 import LogixDriver, CommError
from multiprocessing.pool import ThreadPool
from im_core.helpers import *
import im_core.classes


class Logix:
    def __init__(self, did, address):
        # Settings
        self.id = did
        self.address = address
        self.logger = im_core.classes.Logger()

        # Ignore & whitelist removes many tags we typically are not interested in
        self.tagIgnoreRegex = [re.compile(r"^R.*_S.*_"), re.compile(r"^R.*_.*:.*"), re.compile(r"^raC.*")]
        self.propIgnoreRegex = [re.compile(r"__BitHost.*"), re.compile(r"Cfg_.*"), re.compile(r"PCmd.*"),
                                re.compile(r"MCmd.*"), re.compile(r"Nrdy_.*"), re.compile(r"Rdy_.*"),
                                re.compile(r"Inp_.*"), re.compile(r"OCmd_.*"), re.compile(r"SrcQ.*"),
                                re.compile(r"Err_.*"), re.compile(r"Wrk_.*"), re.compile(r"Inf_.*"),
                                re.compile(r"PSet_.*"), re.compile(r"MSet_.*"), re.compile(r"OSet_.*"),
                                re.compile(r"Set_.*"), re.compile(r"Out_.*"), re.compile(r"Ack_.*"),
                                re.compile(r"P_.*"), re.compile(r"ZZZZZZZZZZ.*")]
        self.dataTypeWhitelist = ['DINT', 'SINT', 'DWORD', 'REAL', 'INT', 'BOOL']

        # Ordered dicts to maintain order
        self.discoveredTags = OrderedDict()
        self.monitoringTags = OrderedDict()

        # Setup
        self._initialize()
        self.discoverTags()

        # Close all of the open connections on program exit
        atexit.register(self._close)

    def _initialize(self):
        # Initialize
        try:
            self.comm = LogixDriver(self.address)
            self.comm.open()
            self.commStack = []
            self.threads = 20
            self.tagsPerRequest = 1000

            i = 0
            while i < self.threads:
                comm = LogixDriver(self.address, init_tags=False)
                comm._tags = self.comm.tags
                comm.open()
                self.commStack.append(comm)
                i += 1
        except CommError:
            self.logger.write('Communication error while initializing PLC driver for source with id ' + str(self.id) + '... trying again in 5 seconds', 'danger')
            time.sleep(5)
            self._initialize()

    def discoverTags(self):
        def get_tags(g_tag, g_obj, dim=0, dimensions=None):
            for prop, sub_obj in g_obj.items():
                hasPropIgnore = any(regex.match(prop) for regex in self.propIgnoreRegex)

                if hasPropIgnore or sub_obj['data_type_name'] not in self.dataTypeWhitelist:
                    continue
                else:
                    g_i = 0
                    if dim > 0:
                        for g_d in dimensions:
                            if g_d > 0:
                                while g_d > g_i:
                                    self.discoveredTags[g_tag + '[' + str(g_i) + ']' + '.' + prop] = sub_obj[
                                        'data_type_name']

                                    if isinstance(sub_obj['data_type'], dict):
                                        get_tags(g_tag + '[' + str(g_i) + ']' + '.' + prop,
                                                 sub_obj['data_type']['internal_tags'])
                                    g_i += 1
                    else:
                        self.discoveredTags[g_tag + '.' + prop] = sub_obj['data_type_name']

                        if isinstance(sub_obj['data_type'], dict):
                            get_tags(g_tag + '.' + prop, sub_obj['data_type']['internal_tags'], dim, dimensions)

        # TODO: What if new tag is added while daemon is already running?
        tags_json = self.comm.tags_json

        for tag_name, obj in tags_json.items():
            if any(regex.match(tag_name) for regex in self.tagIgnoreRegex):
                continue
            else:
                if isinstance(obj['data_type'], dict):
                    get_tags(tag_name, obj['data_type']['internal_tags'], obj['dim'], obj['dimensions'])
                elif obj['dim'] > 0:
                    for d in obj['dimensions']:
                        i = 0
                        if d > 0:
                            while d > i:
                                self.discoveredTags[tag_name + '[' + str(i) + ']'] = obj['data_type_name']
                                i += 1
                else:
                    self.discoveredTags[tag_name] = obj['data_type_name']

    def _getMonitoringTagNamesAsList(self):
        tagNamesList = []
        for tag in self.monitoringTags.values():
            tagNamesList.append(tag.name)
        return tagNamesList

    def _close(self):
        try:
            for comm in self.commStack:
                comm.close()
        except CommError:
            self.logger.write('Failed to close the connection to the PLC on exit for source with id ' + str(self.id), 'danger')


    def _read(self, tags):
        tagValues = []
        comm = None

        try:
            comm = self.commStack.pop()
            tagValues = comm.read(*tags)
        except CommError:
            self.logger.write('Communication error while reading from the PLC for source with id ' + str(self.id) + '... is it offline?', 'danger')
        finally:
            self.commStack.append(comm)

        return tagValues

    def poll(self):
        polledTags = []
        tags = self._getMonitoringTagNamesAsList()

        if len(tags) > 0:
            # Decide if we should run multiple threads to get the results faster
            if len(tags) > self.tagsPerRequest:
                pool = ThreadPool(self.threads)
                chunks = chunkArray(tags, self.tagsPerRequest)

                for chunk in chunks:
                    polledTags.append(pool.apply_async(self._read, args=(chunk,)))

                pool.close()
                pool.join()
                polledTags = [r.get() for r in polledTags]

                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                for thread in polledTags:
                    # Pycomm returns a list of objects when tags to read are >1 in a single poll
                    # When tags to read equals 1, Pycomm returns a single object
                    # Therefore, we do this additional check incase len(tags) - self.tagsPerRequest = 1
                    if isinstance(thread, list):
                        for result in thread:
                            if isValidValue(result.value):
                                self.monitoringTags[result.tag].record(now, result.value)
                    else:
                        if isValidValue(thread.value):
                            self.monitoringTags[thread.tag].record(now, thread.value)
            else:
                now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                polledTags = self._read(tags)
                for result in polledTags:
                    if isValidValue(result.value):
                        self.monitoringTags[result.tag].record(now, result.value)
