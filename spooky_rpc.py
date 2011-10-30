"""
spooky_rpc provides a generic framework for performing remote procedure calls
over a filesystem transport.

Lifetime of a Request
---------------------
1) The SpookyClient writes requests to disk in a well-known directory.  Request
   filenames are guaranteed to be unique, so that concurrent requests can be
   supported without risk of request collisions.  Files are first written to a
   temporary location and then renamed, so that the server never sees incomplete
   request data.

2) The SpookyServer monitors the well-known request directory, and services
   new requests in creation-time order.  Each request is dispatched to the
   user-provided RpcHandler implementation, which is invoked in a new process
   to facilitate request pipelining.  If the RpcHandler provides a response,
   the SpookyServer writes it back to disk in the location expected by the
   client.

3) The SpookyClient optionally polls the expected response location, and
   retrieves the payload when it becomes available.


Caveats
-------
Each request is handled by invoking the RpcHandler.process_request() method
in a new process.  Consequently, the RpcHandler implementation cannot
assume that any program state is maintained between calls to process_request().
"""

import errno, logging, multiprocessing, re, os, time, uuid
import Queue
import binary_rpc as rpc


MESSAGE_FILE_EXT   = '.msg'
PARTIAL_FILE_EXT   = '.part'
MESSAGE_FILE_REGEX = re.compile(r'([0-9a-f]{32})' + re.escape(MESSAGE_FILE_EXT) + '$')

REQUEST_SUBDIR  = 'requests'
RESPONSE_SUBDIR = 'responses'

CLIENT_SLEEP_TIME = 0.1     # idle time between checking for an expected response


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
    request_bytes : string

        Request packet, serialized as a byte sequence.

    response_filename : string

        Path to file where response data should be stored.

    handler : RpcHandler

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


class SpookyServer(object):

    def __init__(self, io_error_response=None, process_count=None, **kwargs):
        """Construct a new server instance.

        Keyword Arguments:
        ------------------
        handler : RpcHandler

            Provides the method for examining a request packet and taking
            action based on its content.

        directory : string

            Directory used for request and response storage.

        log_filename : string

            Location where log file should be stored.

        io_error_response : string or None

            If provided, this byte sequence is returned as the response
            in the event that an I/O error prevents the request from being
            read correctly.

        process_count : int or None
            
            If provided, this is the count of subprocesses to use to service
            requests.  The default is to use the CPU count.
        """
        self.handler           = kwargs['handler']
        self.request_dir       = os.path.join(kwargs['directory'], REQUEST_SUBDIR)
        self.response_dir      = os.path.join(kwargs['directory'], RESPONSE_SUBDIR)
        self.log_filename      = kwargs['log_filename']
        self.io_error_response = io_error_response

        self.log = logging.getLogger('SpookyServer')
        self.log.setLevel(logging.DEBUG)
        handler = logging.FileHandler(self.log_filename)
        formatter = logging.Formatter(fmt='%(asctime)s: %(message)s')
        handler.setFormatter(formatter)
        self.log.addHandler(handler)

        self.log_queue = multiprocessing.Queue()
        self.pool = multiprocessing.Pool(
            processes=process_count,
            initializer=init_subprocess,
            initargs=(self.log_queue,))


    def serve(self, poll_interval=1.0):
        """Process incoming requests indefinitely.

        Parameters:
        -----------
        poll_interval : float

            Length of time to wait between checking for new requests.  Long intervals
            will increase request/response latency, while very short intervals could
            lead to unacceptable I/O activity on remote filesystems.
        """
        for (msg_filename, msg_id) in get_messages(self.request_dir):
            print 'Deleting preexisting request %s...' % str(msg_id)
            try_remove(os.path.join(self.request_dir, msg_filename))


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
                if self.io_error_response:
                    write_file_with_subdirs(response_filename, self.io_error_response)
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



class SpookyTimeoutError(Exception):
    """This exception is raised when a request timeout is exceeded.  The
    'id' attribute contains the identifier for the request which timed out.
    """

    def __init__(self, value):
        self.id = value

    def __str__(self):
        return 'Timeout exceeded for request id %s.' % str(self.id)



class SpookyClient(object):

    def __init__(self, directory):
        """Construct a new client instance, using the specified directory
        for communication with a SpookyServer.
        """
        self.request_dir  = os.path.join(directory, REQUEST_SUBDIR)
        self.response_dir = os.path.join(directory, RESPONSE_SUBDIR)


    def send_request_nowait(self, request_bytes):
        """Send a request packet to the server, without waiting for a response.

        Parameters:
        -----------
        request_bytes : string

            Request packet, serialized as a byte sequence.

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
        string or None

            Response packet, serialized as a byte sequence, or None if the
            response is not yet available.

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


    def wait_response(self, request_id, timeout=None):
        """Wait for a response to arrive for the given request id.

        Parameters:
        -----------
        request_id : int

            Identifier for a request, as provided by send_request_nowait().

        timeout : float or None

            Count of seconds to wait for the response to arrive, or None to wait
            indefinitely.

        Returns:
        --------
        string

            Response packet, serialized as a byte sequence.

        Raises:
        -------
        EnvironmentError, if the response file is available but cannot be read.

        SpookyTimeoutError, if the response was not received within the specified timeout
        interval.
        """
        start_time = time.time()
        while True:
            result = self.check_response(request_id)
            if result is None:
                if (timeout is not None) and (time.time() - start_time > timeout):
                    raise SpookyTimeoutError(request_id)
                time.sleep(CLIENT_SLEEP_TIME)
            else:
                return result


    def send_request_wait(self, request_bytes, timeout=None):
        """Send a request packet to the server, waiting for a response.

        Parameters:
        -----------
        request_bytes : string

            Request packet, serialized as a byte sequence.

        timeout : float or None

            Count of seconds to wait for the response to arrive, or None to wait
            indefinitely.

        Returns:
        --------
        string

            Response packet, serialized as a byte sequence.

        Raises:
        -------
        EnvironmentError, if the request could not be sent or the response could
        not be read.

        SpookyTimeoutError, if the response was not received within the specified timeout
        interval.  (The identifier for the failed request can be retrieved from the
        exception.)
        """
        request_id = self.send_request_nowait(request_bytes)
        return self.wait_response(request_id, timeout)


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



__all__ = [
    'SpookyServer',
    'SpookyClient'
]


