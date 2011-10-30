import os, sys

module_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if module_folder not in sys.path:
    sys.path.append(module_folder)


import time
from spooky_rpc import SpookyClient, SpookyTimeoutError

def test():
    client = SpookyClient('spooky_test_dir')

    print 'test noop'
    client.send_request_nowait('\x00')

    print 'test echo'
    result = client.send_request_wait('\x01')
    assert result == '\x01'

    print 'test timeout exceeded'
    req_id = client.send_request_nowait('\x02')
    try:
        result = client.wait_response(req_id, 1.0)
        assert False    # Should time out
    except SpookyTimeoutError, e:
        result = client.wait_response(req_id)
        assert result == '\x02'

    print 'test timeout not exceeded'
    result = client.send_request_wait('\x02', 5.0)
    assert result == '\x02'

    print 'test orphan responses'
    request_ids = [client.send_request_nowait('\x01') for i in range(5)]
    print 'request_ids: %s' % repr(request_ids)
    time.sleep(5.0)
    orphan_ids = client.purge_responses()
    try:
        assert set(request_ids) == set(orphan_ids)
    except AssertionError:
        print 'orphan_ids: %s' % repr(orphan_ids)
        raise


if __name__ == '__main__':
    test()

