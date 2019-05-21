#!/usr/bin/env python3

import requests
import ujson as json
import time
import sys
import collections

def read_stream(stream):
    mtime = 0
    max_id = None
    queue = collections.deque(maxlen=50)
    while True:
        params = {}
        params['since_mtime'] = mtime
        try:
            r = requests.get('http://stream.meetup.com/2/{}'.format(stream),stream=True,params=params,timeout=300)
            for obj in r.iter_lines():
                if obj:
                    j = json.loads(obj)
                    mtime = j['mtime'] - 1000
                    if 'rsvp_id' in j:
                        id = j['rsvp_id']
                    elif 'photo_id' in j:
                        id = j['photo_id']
                    else:
                        id = j['id']
                    if id in queue:
                        continue
                    queue.append(id)
                    j['ps_type'] = "meetup_{}_stream".format(stream)
                    print(json.dumps(j,sort_keys=True,ensure_ascii=True,escape_forward_slashes=False))
        except KeyboardInterrupt:
            sys.exit()
        except Exception as e:
            time.sleep(3)
            continue


read_stream('photos')
#threading.Thread(target=read_stream, args = ('photos',)).start()
#threading.Thread(target=read_stream, args = ('event_comments',)).start()
#threading.Thread(target=read_stream, args = ('rsvps',)).start()

