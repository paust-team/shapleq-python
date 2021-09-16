class NotEnoughBufferError(Exception):
    def __str__(self):
        return "not enough buffer"


class InvalidChecksumError(Exception):
    def __str__(self):
        return "checksum mismatched"


class MessageDecodeError(Exception):
    def __init__(self, msg="message decode error"):
        self.msg = msg

    def __str__(self):
        return self.msg


class ClientConnectionError(Exception):
    def __init__(self, msg: str):
        self.msg = msg

    def __str__(self):
        return self.msg


class TopicNotSetError(Exception):
    def __str__(self):
        return "topic is not set"


class SocketClosedError(Exception):
    def __str__(self):
        return "socket is closed"


class SocketWriteError(Exception):
    def __init__(self, msg="error occurred while writing data to socket"):
        self.msg = msg

    def __str__(self):
        return self.msg


class SocketReadError(Exception):
    def __init__(self, msg="error occurred while reading data to socket"):
        self.msg = msg

    def __str__(self):
        return self.msg


class InvalidMessageError(Exception):
    def __init__(self, msg="unexpected type of message"):
        self.msg = msg

    def __str__(self):
        return self.msg


class RequestFailedError(Exception):
    def __init__(self, msg="request failed"):
        self.msg = msg

    def __str__(self):
        return self.msg