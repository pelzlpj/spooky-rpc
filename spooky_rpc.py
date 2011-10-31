################################################################################
# spooky_rpc
#
# Copyright (c) 2011 Paul Pelzl
# All rights reserved.
# 
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
# 
#   Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
# 
#   Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
# 
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
################################################################################

"""
spooky_rpc provides a generic framework for performing remote procedure calls
over a filesystem transport.

Lifetime of a Request
---------------------
1) The Client writes requests to disk in a well-known directory.  Request
   filenames are guaranteed to be unique, so that concurrent requests can be
   supported without risk of request collisions.  Files are first written to a
   temporary location and then renamed, so that the server never sees incomplete
   request data.

2) The Server monitors the well-known request directory, and services
   new requests in creation-time order.  Each request is dispatched to the
   user-provided BinaryRequestHandler implementation, which is invoked in a new
   process to facilitate request pipelining.  If the BinaryRequestHandler
   provides a response, the Server writes it back to disk in the location
   expected by the client.

3) The Client optionally polls the expected response location, and
   retrieves the payload when it becomes available.


Caveats
-------
* Each request is handled by invoking the BinaryRequestHandler.process_request()
  method in a new process.  Consequently, the RequestHandler implementation
  cannot assume that any changes in the program state are maintained across
  multiple calls to process_request().
"""

import abc, errno, logging, multiprocessing, re, os, sys, time, unittest, uuid
import Queue

VERSION = '1.0.0'

MESSAGE_FILE_EXT   = '.msg'
PARTIAL_FILE_EXT   = '.part'
MESSAGE_FILE_REGEX = re.compile(r'([0-9a-f]{32})' + re.escape(MESSAGE_FILE_EXT) + '$')

REQUEST_SUBDIR  = 'requests'
RESPONSE_SUBDIR = 'responses'


def get_messages(path):
    """Get a list of (message_filename, message_id) for messages found the given path.
    The list is returned in message_mtime order.
    """
    try:
        all_files = os.listdir(path)
    except OSError:
        return []

    items = []
    for f in all_files:
        m = MESSAGE_FILE_REGEX.match(f)
        if m:
            fq_file = os.path.join(path, f)
            try:
                st = os.stat(fq_file)
                items.append( (f, uuid.UUID(hex=m.group(1)), st.st_mtime) )
            except OSError:
                pass
    items.sort(key=lambda (x, y, mtime): mtime)

    return [(filename, id) for (filename, id, mtime) in items]


def make_msg_filename(message_id):
    """Constructs a message filename from the given message id."""
    return message_id.hex + MESSAGE_FILE_EXT


def try_remove(path):
    """Remove a file, ignoring errors."""
    try:
        os.remove(path)
    except OSError:
        pass


def try_makedirs(directory):
    """Tries to create all components of the given directory, ignoring errors."""
    try:
        os.makedirs(directory)
    except OSError:
        pass


def fancy_rename(src, dst):
    """Stupid Win32 os.rename() will not overwrite the destination."""
    try_remove(dst)
    os.rename(src, dst)


def write_file_with_subdirs(filename, content):
    """Write a file, creating any intermediate subdirectories."""
    try_makedirs(os.path.dirname(filename))
    
    # We don't want the recipient to see partial messages, so
    # we write to a tempfile and then rename when done.
    tmp_filename =  filename + PARTIAL_FILE_EXT
    with open(tmp_filename, 'wb') as f:
        f.write(content)
    fancy_rename(tmp_filename, filename)


def handle_request(**kwargs):
    """Handle a request.  (This function is invoked as the target for
    multiprocessing.Process().)

    Keyword Arguments:
    ------------------
    request_bytes : str/bytes

        Binary request packet.

    response_filename : string

        Path to file where response data should be stored.

    handler : BinaryRequestHandler

        Client code for handling the request.

    Attributes:
    -----------
    log_queue : multiprocessing.Queue

        Queue to which error strings should be sent.  (This is a hack.  When
        using multiprocess.Pool, queues cannot be passed as function arguments;
        they must be passed in the process initializer.)

    """
    request_bytes     = kwargs['request_bytes']
    response_filename = kwargs['response_filename']
    handler           = kwargs['handler']

    response_bytes = handler.process_request(request_bytes)
    if response_bytes:
        try:
            write_file_with_subdirs(response_filename, response_bytes)
        except EnvironmentError, e:
            handle_request.log_queue.put(
                'Unable to write response \"%s\": %s' % (response_filename, str(e)))


def init_subprocess(queue):
    handle_request.log_queue = queue


class BinaryRequestHandler(object):
    """This class shall be overridden to provide server-side logic for
    processing requests.  The interface requires that requests and responses are
    delivered in a binary format, so the implementation must take responsibility
    for the serialization/deserialization work.
    """

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def process_request(self, req):
        """Perform the processing specified in the request data.

        Exceptions shall not be raised.  If error conditions must be signalled, then
        the binary response protocol must be designed to carry error information.

        The implementation should not expect that any program state is maintained
        across multiple invocations of process_request(); the caller may invoke
        the method concurrently in separate processes.

        Parameters:
        -----------
        req : str/bytes
        
            Binary request packet.

        Returns:
        --------
        str/bytes or None

            If the request packet requires a response, then the the return value
            is the binary response packet; otherwise, None is returned.
        """
        return

    @abc.abstractproperty
    def io_error_response(self):
        """Optional binary response packet which should be returned in the event
        that the server cannot read an incoming request due to an I/O error.

        Returns:
        --------
        str/bytes or None
        """
        return


class Server(object):

    def __init__(self, process_count=None, **kwargs):
        """Construct a new server instance.

        Keyword Arguments:
        ------------------
        handler : BinaryRequestHandler

            Provides the method for examining a binary request packet and taking
            action based on its content.
 
        directory : string

            Directory used for request and response storage.

        log_filename : string

            Location where log file should be stored.

        process_count : int or None
            
            If provided, this is the count of subprocesses to use to service
            requests.  The default is to use the CPU count.
        """
        self.handler       = kwargs['handler']
        self.request_dir   = os.path.join(kwargs['directory'], REQUEST_SUBDIR)
        self.response_dir  = os.path.join(kwargs['directory'], RESPONSE_SUBDIR)
        self.log_filename  = kwargs['log_filename']
        self.process_count = process_count

        assert isinstance(self.handler, BinaryRequestHandler)

        self.log = logging.getLogger('SpookyServer')
        self.log.setLevel(logging.DEBUG)
        handler = logging.FileHandler(self.log_filename)
        formatter = logging.Formatter(fmt='%(asctime)s: %(message)s')
        handler.setFormatter(formatter)
        self.log.addHandler(handler)


    def serve(self, poll_interval=1.0):
        """Process incoming requests indefinitely.

        Parameters:
        -----------
        poll_interval : float

            Length of time to wait between checking for new requests, in
            seconds.  Long intervals will increase request/response latency,
            while very short intervals could lead to unacceptable I/O activity
            on remote filesystems.
        """
        self.log.info('Server startup, listening at %s .' % self.request_dir)
        for (msg_filename, msg_id) in get_messages(self.request_dir):
            self.log.info('Deleting preexisting request %s...' % str(msg_id))
            try_remove(os.path.join(self.request_dir, msg_filename))

        self.log_queue = multiprocessing.Queue()
        self.pool = multiprocessing.Pool(
            processes=self.process_count,
            initializer=init_subprocess,
            initargs=(self.log_queue,))


        while True:
            # Check for any logging messages from subprocesses
            try:
                while True:
                    subprocess_error = self.log_queue.get_nowait()
                    self.log.error(subprocess_error)
            except Queue.Empty:
                pass

            is_message_processed = False
            for (f, req_id) in get_messages(self.request_dir):
                is_message_processed = True
                self._serve_one(f, req_id)

            # Don't sleep unless there was no work available
            if not is_message_processed:
                time.sleep(poll_interval)


    def _serve_one(self, msg_filename, req_id):
        """Service a single request."""
        request_filename  = os.path.join(self.request_dir, msg_filename)
        response_filename = os.path.join(self.response_dir, msg_filename)

        try:
            with open(request_filename, 'rb') as request_file:
                request_bytes = request_file.read()
        except EnvironmentError, e:
            self.log.error('Unable to read request \"%s\": %s' %
                (request_filename, str(e)))
            try:
                if self.handler.io_error_response is not None:
                    write_file_with_subdirs(response_filename, self.handler.io_error_response)
            except EnvironmentError, e:
                self.log.error('Unable to write io_error_response \"%s\": %s' %
                    (response_filename, str(e)))
            return
        try_remove(request_filename)

        self.pool.apply_async(handle_request, (), {
                'request_bytes'     : request_bytes,
                'response_filename' : response_filename,
                'handler'           : self.handler,
            })



class TimeoutError(Exception):
    """This exception is raised when a request timeout is exceeded.  The
    'id' attribute contains the identifier for the request which timed out.
    """

    def __init__(self, value):
        self.id = value

    def __str__(self):
        return 'Timeout exceeded for request id %s.' % str(self.id)



class Client(object):

    def __init__(self, directory):
        """Construct a new client instance, using the specified directory
        for communication with a Server.
        """
        self.request_dir  = os.path.join(directory, REQUEST_SUBDIR)
        self.response_dir = os.path.join(directory, RESPONSE_SUBDIR)


    def send_request_nowait(self, request_bytes):
        """Send a request packet to the server, without waiting for a response.

        Parameters:
        -----------
        request_bytes : str/bytes

            Binary request packet.

        Returns:
        --------
        UUID

            Request identifier, intended for use with check_response() or
            wait_response().

        Raises:
        -------
        EnvironmentError, if the request could not be sent
        """
        request_id       = uuid.uuid1()
        request_filename = os.path.join(self.request_dir, make_msg_filename(request_id))
        write_file_with_subdirs(request_filename, request_bytes)
        return request_id


    def check_response(self, request_id):
        """Check whether a response is available for the given request id.

        Note: if a response is successfully retrieved, then the request_id is
        invalidated.  (Request identifiers are not guaranteed to be unique
        across the program lifetime.)

        Parameters:
        -----------
        request_id : UUID

            Identifier for a request, as provided by send_request_nowait().

        Returns:
        --------
        str/bytes or None

            Binary response packet, or None if the response is not yet
            available.

        Raises:
        -------
        EnvironmentError, if a response file is available but cannot be read.
        """
        response_file = os.path.join(self.response_dir, make_msg_filename(request_id))
        try:
            with open(response_file, 'rb') as f:
                response_bytes = f.read()
            try_remove(response_file)
            return response_bytes
        except IOError, e:
            if e.errno == errno.ENOENT:
                return None
            else:
                raise


    def wait_response(self, request_id, timeout=0.0, poll_interval=0.1):
        """Wait for a response to arrive for the given request id.

        Parameters:
        -----------
        request_id : int

            Identifier for a request, as provided by send_request_nowait().

        timeout : float

            If timeout > 0, then it specifies the maximum number of seconds to
            wait for the response to arrive; otherwise, wait indefinitely.

        poll_interval : float

            Length of time to wait between response checks, in seconds.

        Returns:
        --------
        str/bytes

            Binary response packet.

        Raises:
        -------
        EnvironmentError, if the response file is available but cannot be read.

        TimeoutError, if the response was not received within the specified timeout
        interval.
        """
        start_time = time.time()
        while True:
            result = self.check_response(request_id)
            if result is None:
                if timeout > 0.0 and (time.time() - start_time > timeout):
                    raise TimeoutError(request_id)
                time.sleep(poll_interval)
            else:
                return result


    def send_request_wait(self, request_bytes, timeout=0.0, poll_interval=0.1):
        """Send a request packet to the server, waiting for a response.

        Parameters:
        -----------
        request_bytes : str/bytes

            Binary request packet.

        timeout : float

            If timeout > 0, then it specifies the maximum number of seconds to
            wait for the response to arrive; otherwise, wait indefinitely.

        poll_interval : float

            Length of time to wait between response checks, in seconds.

        Returns:
        --------
        str/bytes

            Binary response packet.

        Raises:
        -------
        EnvironmentError, if the request could not be sent or the response could
        not be read.

        TimeoutError, if the response was not received within the specified timeout
        interval.  (The identifier for the failed request can be retrieved from the
        exception.)
        """
        request_id = self.send_request_nowait(request_bytes)
        return self.wait_response(request_id, timeout, poll_interval)


    def purge_responses(self):
        """Delete all response files.

        In general, response files will fail to be automatically deleted whenever the
        client does not wait for them.  This will occur, for example, if
        send_request_wait() times out or if the client invokes send_request_nowait()
        without polling check_response() until the response is received.

        Returns:
        --------
        list of UUID

            List of request ids corresponding to the deleted response messages.
        """
        result = []
        for (msg_filename, msg_id) in get_messages(self.response_dir):
            try_remove(os.path.join(self.response_dir, msg_filename))
            result.append(msg_id)
        return result


################################################################################
# UNIT TESTS
################################################################################

REQ_NOOP   = '\x00'  # Ask server to do nothing
REQ_PING   = '\x01'  # Ask server to immediately respond
REQ_SLEEP3 = '\x02'  # Ask server to idle for 3 sec before responding

RESP_BAD_DATA = '\xfe'
RESP_IO_ERROR = '\xff'

TEST_DIR = 'spooky_test_dir'

class TestHandler(BinaryRequestHandler):

    def process_request(self, req):
        if req == REQ_NOOP:
            sys.stdout.write('received noop\n')
            return None
        elif req == REQ_PING:
            sys.stdout.write('received ping\n')
            return REQ_PING
        elif req == REQ_SLEEP3:
            sys.stdout.write('received sleep(3)\n')
            time.sleep(3.0)
            return REQ_SLEEP3
        else:
            sys.stdout.write('received unhandled bytes: %s' % req.encode('hex'))
            return RESP_BAD_DATA

    def io_error_response(self):
        return RESP_IO_ERROR


def start_test_server():
    server = Server(
        handler=TestHandler(),
        directory=TEST_DIR,
        log_filename='test-server.log',
        process_count=16)
    server.serve()


class SpookyTests(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls._proc = multiprocessing.Process(target=start_test_server)
        cls._proc.start()
        cls._client = Client(TEST_DIR)
        cls._client.purge_responses()

    @classmethod
    def tearDownClass(cls):
        cls._proc.terminate()
        cls._proc.join()
            
    def test_noop(self):
        req_id = self._client.send_request_nowait(REQ_NOOP)
        self.assertTrue(isinstance(req_id, uuid.UUID),
            msg='send_request_nowait() shall return a UUID')

    def test_ping(self):
        response = self._client.send_request_wait(REQ_PING)
        self.assertEqual(response, REQ_PING,
            msg=('unexpected response received from REQ_PING: 0x%s' % response.encode('hex')))

    def test_timeout_exceeded(self):
        req_id = self._client.send_request_nowait(REQ_SLEEP3)
        try:
            response = self._client.wait_response(req_id, 1.0)
            assert False, 'received response when timeout was expected'
        except TimeoutError:
            response = self._client.wait_response(req_id)
            self.assertEqual(response, REQ_SLEEP3,
                msg=('unexpected response after REQ_SLEEP3: 0x%s' % response.encode('hex')))

    def test_timeout_not_exceeded(self):
        response = self._client.send_request_wait(REQ_SLEEP3, 5.0)
        self.assertEqual(response, REQ_SLEEP3,
            msg=('unexpected response after REQ_SLEEP3: 0x%s' % response.encode('hex')))

    def test_concurrent_requests(self):
        # Eight concurrent requests, each of which should sleep for 3 sec
        request_ids = [self._client.send_request_nowait(REQ_SLEEP3) for i in range(8)]
        time.sleep(4.0)
        # All requests should now be complete
        for id in request_ids:
            self.assertEqual(self._client.check_response(id), REQ_SLEEP3)

    def test_orphan_responses(self):
        request_ids = [self._client.send_request_nowait(REQ_PING) for i in range(5)]
        time.sleep(2.0)
        orphan_ids = self._client.purge_responses()
        self.assertEqual(set(request_ids), set(orphan_ids),
            msg=('purge_responses() removed unexpected set of responses'))


if __name__ == '__main__':
    (major, minor, x, y, z) = sys.version_info
    assert major == 2 and minor >= 7, 'Unit tests require Python 2.7.'
    unittest.main()


__all__ = [
    'BinaryRequestHandler',
    'Server',
    'Client',
    'TimeoutError'
]


