import os, sys

module_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if module_folder not in sys.path:
    sys.path.append(module_folder)


import time
from spooky_rpc import SpookyClient, SpookyTimeoutError

NOOP  = '\x00'
ECHO  = '\x01'
SLEEP = '\x02'


def test():
    client = SpookyClient('spooky_test_dir')
    client.purge_responses()

    print 'test noop'
    client.send_request_nowait(NOOP)

    print 'test echo'
    result = client.send_request_wait(ECHO)
    assert result == ECHO

    print 'test timeout exceeded'
    req_id = client.send_request_nowait(SLEEP)
    try:
        result = client.wait_response(req_id, 1.0)
        assert False    # Should time out
    except SpookyTimeoutError, e:
        result = client.wait_response(req_id)
        assert result == SLEEP

    print 'test timeout not exceeded'
    result = client.send_request_wait(SLEEP, 5.0)
    assert result == SLEEP

    print 'test concurrent requests'
    # 10 concurrent requests, each of which should sleep for 3 sec
    request_ids = [client.send_request_nowait(SLEEP) for i in range(10)]
    time.sleep(4.0)
    # All 10 should now be complete
    for id in request_ids:
        assert client.check_response(id) == SLEEP

    print 'test orphan responses'
    request_ids = [client.send_request_nowait(ECHO) for i in range(5)]
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

