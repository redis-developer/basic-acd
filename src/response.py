from enum import Enum

class RESPONSE_TYPE(Enum):
    OK = 200
    QUEUED = 202
    LOCKED = 409
    ERR = 400

class Response(object):
    def __init__(self, resp_type, result=None):
        self.resp_type: RESPONSE_TYPE = resp_type
        self.result: str = result
    
    def __str__(self):
        return f'resp_type: {self.resp_type}, result: {self.result}'