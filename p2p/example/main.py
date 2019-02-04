from demo_client import DemoClientPeer, DemoClient
from demo_server import DemoServerPeer, DemoServer

c = DemoClient(DemoClientPeer())
s = DemoServer(DemoServerPeer())

c.send_file('test.txt')
s.send_file('test.jpg')
