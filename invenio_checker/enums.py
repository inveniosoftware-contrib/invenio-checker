from enum import Enum


class StatusWorker(Enum):
    dead_parrot = 0
    scheduled = 1
    booting = 2
    ready = 3
    running = 4
    ran = 5
    failed = 6
    committed = 7


class StatusMaster(Enum):
    unknown = 0
    booting = 1
    running = 2
    waiting_to_commit = 3
    failed = 4
    completed = 5
