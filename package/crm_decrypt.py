import hashlib

import math
from Crypto.Cipher import DES3
import base64
import json
from package.logging_helper import logger
from package.db_helper import config
## this module is for CRM database email and phone decryption
## VFX and VT using the decode with key for email and phone
## VCN using the decode with key for email and decode without key for phone
# config["encryption_key"]

def hex(key):
    m = hashlib.md5()
    m.update(key)
    f = m.hexdigest()  # ee4189bbf0ad1d6056d517ac1705a288
    # get the first 24 bytes
    return f[0:24]  # bytearray(f)[0:24]


def encode_decode(src, key, mode):
    """ if mode is "ENCODE"

        encode src with key and return the encoded data
        
        if key is None, using the BASE64 to do the encode

        else using the DES3 to do the encode (encode with DES followed by BASE64 )

        if mode is "DECODE"

        decode src with key and return the decoded data

        if key is None, using the BASE64 to do the decode

        else using the DES3 to do the decode (decode with BASE64 followed by DES3)

        For DES3, using the ECB mode with PKCS7 padding mode; key should be 192 bits
        
    """

    # if src is None or src == "":
    #     return ""
    try:
        if key is None:
            # using the BASE64
            if mode == "ENCODE":

                return base64.b64encode(src.encode("utf-8")).decode("utf-8")

            elif mode == "DECODE":

                return base64.b64decode(src.encode("utf-8")).decode("utf-8")
        else:
            enk = hex(key.encode("utf-8"))

            BS = DES3.block_size

            pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)

            unpad = lambda s: s[0:-ord(s[-1])]

            cipher = DES3.new(enk, DES3.MODE_ECB)

            if mode == "ENCODE":
                #src = src.decode("utf-8")
                src = pad(src)
                encoded = cipher.encrypt(src.encode("utf-8"))

                result = base64.b64encode(encoded)

                return result

            elif mode == "DECODE":

                src_after_base64 = base64.b64decode(src)
                src_decoded = cipher.decrypt(src_after_base64).decode("utf-8")
                result = unpad(src_decoded)

                return result
    except:
        try:
            logger.exception("Fail to do the encode_decode with input: " + src)
        except:
            logger.exception("Fail to do the encode_decode with input: NAN")
        return src
        # raise Exception("Fail to do the encode_decode with input: " + src )


def decode_with_key(src, mode='vfx'):
    if src is None:
        return src

    if mode == 'OPL':
        key = config["encryption_key"]
    else:
        key = config["encryption_key"]
    return encode_decode(src, key, "DECODE")

def encode_with_key(src):
    if src is None:
        return src

    key =  config["encryption_key"]
    data = encode_decode(src, key, "ENCODE")
    if isinstance(data,str) or isinstance(data,int):
        return data
    else:
        return data.decode("utf-8")


def decode_without_key(src):
    if src is None:
        return src

    return encode_decode(src, None, "DECODE")


if __name__ == "__main__":
    # test des3
    key =  config["encryption_key"]

    plaintext = "ww.elijah@clement.com"

    ciphertext = encode_decode(plaintext, key, "ENCODE")

    new_plaintext = encode_decode(ciphertext, key, "DECODE")

    assert plaintext == new_plaintext

    # test base64

    plaintext = "ww.elijah@clement.com"

    ciphertext = encode_decode(plaintext, None, "ENCODE")

    new_plaintext = encode_decode(ciphertext, None, "DECODE")

    assert plaintext == new_plaintext
