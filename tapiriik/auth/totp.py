import time
import base64
import math
import hmac
import hashlib
import struct


class TOTP:
    def Get(secret):
        counter = struct.pack(">Q", int(time.time() / 30))
        key = base64.b32decode(secret.upper().encode())
        csp = hmac.new(key, counter, hashlib.sha1)
        res = csp.digest()
        offset = res[19] & 0xf
        code_pre = struct.unpack(">I", res[offset:offset + 4])[0]
        code_pre = code_pre & 0x7fffffff
        return int(code_pre % (math.pow(10, 6)))
