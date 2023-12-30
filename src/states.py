from enum import Enum

class AGENT_STATE(Enum):
    AVAILABLE = 1
    UNAVAILABLE = 0

class ACD_STATE(Enum):
    OPEN = 1
    CLOSED = 0

class CONTACT_STATE(Enum):
    QUEUED = 1
    ASSIGNED = 2
    COMPLETE = 3