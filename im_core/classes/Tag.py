class Tag:
    def __init__(self, tid, name):
        # Settings
        self.id = tid
        self.name = name
        self.records = []

    def record(self, time, value):
        self.records.append((self.id, time, value))
