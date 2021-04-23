import socket
import time
 
class SBSConnection:

    def __init__(self, ip="127.0.0.1", port="30003"):

        self.buff = b''
        self.ip = ip
        self.port = int(port)
        self.connect()

    def connect(self):
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPIDLE, 60)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPCNT, 4)
        self.socket.setsockopt(socket.SOL_TCP, socket.TCP_KEEPINTVL, 15)
        self.socket.settimeout(10)
        try:
            self.socket.connect((self.ip, self.port))
            print("Connected")
        except:
            print ("Connection to " + self.ip + " " + str(self.port) + " failed.")
            time.sleep(1)

    def reconnect(self):
        print("Reconnecting ...")
        try:
            self.socket.close()
        except:
            pass
        self.connect()

    def read_line(self):
        data = b'\n'
        while not data in self.buff:
            req = ''
            while True:
                try:
                    req = self.socket.recv(10)
                    if not req or req == '':
                        print('Connection closed by peer')
                        self.reconnect()
                    break

                except socket.timeout:
                    continue

                except:
                    print ('Other Socket err, exit and try creating socket again')
                    self.reconnect()
                    break
            self.buff += req
 
        pos = self.buff.find(data)
        rval = self.buff[:pos + len(data)]
        self.buff = self.buff[pos + len(data):]
 
        return rval.decode('utf-8')
