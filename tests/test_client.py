import socket
import time
import json

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect(('127.0.0.1', 8999))
while True:
    try:
        Name = s.getsockname()[0] + ":" + str(s.getsockname()[1])
        dict = {}
        time1 = time.time()
        print(time1)
        dict[Name] = str(time1)
        byte = json.dumps(dict).encode("utf-8")
        s.send(byte)
        time.sleep(3)
    except Exception as e:
        print(e)
        s.close()
        break