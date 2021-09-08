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
