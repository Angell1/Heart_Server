import collections
import threading
import time
import socket
import selectors
import json
from core import ThreadPool

"""
全局路由及状态表
127.0.0.1:63041:{'status': 'online', 'time': '1594732256.146042', 'index': 0}

127.0.0.1:63042:{'status': 'online', 'time': '1594732236.146042', 'index': 1}

"""
class Global():
    def __init__(self):
        self.Globaltable = {}

    def keyisexist(self,key):
        if key in self.Globaltable:
            return True
        return  False

    def getvalue(self,key):
        if(self.keyisexist(key)):
            return self.Globaltable[key]

"""


"""
class QueuePool():
    def __init__(self,PoolMaxSize):
        self.pool = {}
        self.PoolMaxSize = PoolMaxSize
        self.PoolSize = 0
        self.Queuearr = []

    def add(self,Name,queue):
        if self.PoolMaxSize > self.PoolSize:
            if Name in self.pool:
                print("此队列名已存在队列中！！！")
                return
            else:
                self.pool[Name] = queue
                self.PoolSize += 1
                self.Queuearr.append(Name)
        else:
            print("队列池已满，无法添加队列！！！")
            return

    def delete(self,key):
        if key in self.pool:
            self.pool.pop(key)
            self.PoolSize -= 1
            self.Queuearr.pop(key)

    def run(self):
        threadlist = []
        if (self.PoolSize <= 0):
            print("队列池中无队列可用！！！")
            return
        for key in self.pool.keys():
            t = threading.Thread(target=self.read, args=(key,))
            threadlist.append(t)
        for t in threadlist:
            t.start()

    def clear(self):
        self.pool.clear()
        self.PoolSize = 0
        self.Queuearr.clear()

    def read(self,key):
        while True:
            time.sleep(5)
            self.pool[key].lock.acquire()
            for i in self.pool[key].queue:
                print("遍历到的队列值：",i)
                for lkey in i.keys():
                    subtractvalue = time.time() - float(i[lkey])
                    print("%s的过期时间计算：%s" % (lkey,subtractvalue))
                    if (subtractvalue >= 5):
                        Globaltable.Globaltable[lkey]['status'] = "down"
                    else:
                        Globaltable.Globaltable[lkey]['status'] = "online"
                    Globaltable.Globaltable[lkey]['time'] = str(time.time())
                    print("%s:%s" % (lkey,Globaltable.Globaltable[lkey]))
            self.pool[key].lock.release()


class Queue():
    def __init__(self,Maxsize,):
        self.queue = collections.deque(maxlen = Maxsize)
        self.lock = threading.Lock()




def distribute_data(conn,data):
    #获取本地ip和端口
    # print(conn.getsockname())
    # 获取对端ip和端口
    # print(conn.getpeername())
    try:
        key = conn.getpeername()[0] + ":" +str(conn.getpeername()[1])
        hashdiegst = hash(key)
        poolindex = hashdiegst % Pool.PoolSize
        # print("获取队列索引为：",poolindex)
        # print("获取队列名为：",Pool.Queuearr[poolindex])
        # print("向队列添加值为:",data)
        Pool.pool[Pool.Queuearr[poolindex]].lock.acquire()
        Pool.pool[Pool.Queuearr[poolindex]].queue.append(data)
        newqueueindex = len(Pool.pool[Pool.Queuearr[poolindex]].queue) - 1
        """{
            127.0.0.1:57678:{
                    status:xx,
                    time:xx,
                    index:xx,
                }
            }
        """
        if Globaltable.keyisexist(key):
            # print("%s更新index,更新的时间为：%s"% (key,data[key]))
            oldqueueindex = Globaltable.Globaltable[key]["index"]
            del Pool.pool[Pool.Queuearr[poolindex]].queue[oldqueueindex]
            count = 0
            for v in Pool.pool[Pool.Queuearr[poolindex]].queue:
                Globaltable.Globaltable[list(v.keys())[0]]["index"] = count
                count += 1
        else:
            Globaltable.Globaltable[key] = {"status":"online","time":data[key],"index":newqueueindex}
        Pool.pool[Pool.Queuearr[poolindex]].lock.release()
    except Exception as e:
        print(e)


def parser_data(data):
    dict = json.loads(data)
    return dict

def accept(sock, mask):
    conn, addr = sock.accept()  # Should be ready
    conn.setblocking(False)  # 设定非阻塞
    sel.register(conn, selectors.EVENT_READ, read)  # 新连接注册read回调函数

def read(conn, mask):
    try:
        conn.setblocking(True)
        data = conn.recv(1024)  # Should be ready
        if data:
            data = data.decode("utf-8")
            #解析数据
            data = parser_data(data)
            #分发数据及在哈希表中操作
            distribute_data(conn,data)
        # else:
        #     print('closing', conn)
        #     sel.unregister(conn)
        #     conn.close()
    except Exception as e:
        print(e)
        sel.unregister(conn)
        conn.close()
    return


if __name__ == '__main__':
    Globaltable = Global()
    #创建队列池
    Pool = QueuePool(3)
    #创建三个队列并添加到队列池
    q1 = Queue(1000)
    q2 = Queue(1000)
    q3 = Queue(1000)
    Pool.add("q1", q1)
    Pool.add("q2", q2)
    Pool.add("q3", q3)
    # selectors模块默认会用epoll，如果你的系统中没有epoll(比如windows)则会自动使用select
    # 生成一个select对象
    sel = selectors.DefaultSelector()
    sock = socket.socket()
    sock.bind(('localhost', 8999))
    sock.listen()
    sock.setblocking(False)
    sel.register(sock, selectors.EVENT_READ, accept)  # 把刚生成的sock连接对象注册到select连接列表中，并交给accept函数处理
    #运行队列池
    Pool.run()
    # 创建线程池
    executor = ThreadPool.ThreadPool(10)
    while True:
        # 默认是阻塞，有活动连接（触发事件）就返回活动的连接列表
        events = sel.select()
        # 这里看起来是select，其实有可能会使用epoll，如果你的系统支持epoll，那么默认就是epoll
        for key, mask in events:
            callback = key.data  # 去调accept函数或者read函数
            if callback.__name__ == "read":
                # print("%s触发了read：" % str(key.fileobj))
                # threading.Thread(target=read, args=(key.fileobj, mask)).start()
                executor.put(read, (key.fileobj,mask),None)
            else:
                # key.fileobj就是readable中的一个socket连接对象
                callback(key.fileobj, mask)