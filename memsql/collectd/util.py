import hashlib

def hash_64_bit(*values):
    result = hashlib.sha1('.'.join(values))
    # truncate into a 64-bit int
    return int(result.hexdigest()[:16], 16)
