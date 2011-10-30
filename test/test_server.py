import os, sys

module_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if module_folder not in sys.path:
    sys.path.append(module_folder)


import time
from binary_rpc import RpcHandler
from spooky_rpc import SpookyServer


class TestHandler(RpcHandler):

    def process_request(self, req):
        """Perform the processing specified in the request data."""
        if req == '\x00':
            print 'noop'
        elif req == '\x01':
            print 'echo'
            return '\x01'
        elif req == '\x02':
            print 'sleep'
            time.sleep(3.0)
            return '\x02'
        else:
            print 'Unhandled bytes: %s' % req.encode('hex')


if __name__ == '__main__':
    handler = TestHandler()
    server = SpookyServer(handler, 'spooky_test_dir', '\xff')
    server.serve()


