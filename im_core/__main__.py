from im_core.classes import Daemon
from twisted.internet import task, reactor

def poll():
    Daemon.pollSources()


def store():
    Daemon.storeData()


def sync():
    Daemon.syncDaemon()
    Daemon.syncSources()


def forward():
    Daemon.forwardData()


def utilities():
    Daemon.forwardLogs()


if __name__ == '__main__':
    # Initialize
    Daemon = Daemon()

    # Scheduling
    task.LoopingCall(poll).start(Daemon.poll_time)
    task.LoopingCall(store).start(Daemon.store_time, False)
    task.LoopingCall(sync).start(Daemon.sync_time, False)
    task.LoopingCall(forward).start(Daemon.forward_time, False)
    task.LoopingCall(utilities).start(5, False)
    reactor.run()
