

import datetime
import random  
import time 


#==============Util variables===================#
QUEUE_DB_PATH = "./queue.db"
INTERVAL= 1.0
BACKOFF=2 
JITTER =0.1 
#==============Util variables===================#


#==============Time Utilities===================#
def now_iso():
    return datetime.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def parse_iso(s):
    return datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))

def iso_to_unix(s):
    return int(parse_iso(s).timestamp())

def unix_now():
    return int(datetime.datetime.utcnow().timestamp())

def compute_next_run(attempts, base=BACKOFF):
    # exponential backoff with jitter
    backoff = base * (2 ** attempts)
    jitter = backoff * JITTER
    delay = backoff + random.uniform(-jitter, jitter)
    return int(time.time() + max(1, int(delay)))
#==============Time Utilities===================#




