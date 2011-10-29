import errno, logging, re, os, time
import binary_rpc as rpc


MESSAGE_FILE_REGEX  = re.compile(r'(\d{8})\.msg$')
MESSAGE_FILE_FORMAT = '%08d.msg'

FAILURE_EXT     = '.fail'
REQUEST_SUBDIR  = 'requests'
RESPONSE_SUBDIR = 'responses'

SERVER_SLEEP_TIME = 1.0     # idle time between checking for new requests
CLIENT_SLEEP_TIME = 0.1     # idle time between checking for an expected response


def get_messages(path):
    """Returns: ordered list of (message_filename, message_generation_count) contained
    in the given path."""
    try:
        all_files = os.listdir(path)
    except OSError:
        return []

    all_files.sort()

    result = []
    for f in all_files:
        m = MESSAGE_FILE_REGEX.match(f)
        if m:
            result.append( (f, int(m.group(1))) )
    return result


def get_generation_count(path):
    """Compute the largest message index of all messages in the given path.
    If there are no messages, the result is 0.

    Returns: int
    """
    try:
        (filename, gen_count) = get_messages(path)[-1]
    except IndexError:
        return 0
    return gen_count


def make_msg_filename(gen_count):
    """Constructs a message filename which bears the given generation count."""
    return MESSAGE_FILE_FORMAT % gen_count


def try_remove(path):
    """Remove a file, ignoring errors."""
    try:
        os.remove(path)
    except OSError:
        pass


def fancy_rename(src, dst):
    """Stupid Win32 os.rename() will not overwrite the destination."""
    try_remove(dst)
    os.rename(src, dst)


def write_file_with_subdirs(filename, content):
    """Write a file, creating any intermediate subdirectories."""
    try:
        os.makedirs(os.path.dirname(filename))
    except OSError:
        pass
    
    # We don't want the recipient to see partial messages, so
    # we write to a tempfile and then rename when done.
    tmp_filename = filename + ".part"
    with open(tmp_filename, 'wb') as f:
        f.write(content)
    fancy_rename(tmp_filename, filename)


class SpookyServer(object):

    def __init__(self, handler, directory, io_error_response=None):
        """Construct a new server instance.

        Parameters:
        -----------
        handler : RpcHandler

            Provides the method for examining a request packet and taking
            action based on its content.

        directory : string

            Directory used for request and response storage

        io_error_response : string or None

            If provided, this byte sequence is returned as the response
            in the event that an I/O error prevents the request from being
            read correctly.
        """
        self.handler           = handler
        self.request_dir       = os.path.join(directory, REQUEST_SUBDIR)
        self.response_dir      = os.path.join(directory, RESPONSE_SUBDIR)
        self.io_error_response = io_error_response

        self.log = logging.getLogger('SpookyServer')
        self.log.setLevel(logging.DEBUG)
        self.log.addHandler(logging.FileHandler('spooky-server.log'))


    def serve(self, poll_interval=1.0):
        """Process incoming requests indefinitely.

        Parameters:
        -----------
        poll_interval : float

            Length of time to wait between checking for new requests.  Long intervals
            will increase request/response latency, while very short intervals could
            lead to unacceptable I/O activity on remote filesystems.
        """

        self.generation = get_generation_count(self.request_dir)

        while True:
            is_message_processed = False
            for (f, gen_count) in get_messages(self.request_dir):
                request_filename  = os.path.join(self.request_dir, f)
                response_filename = os.path.join(self.response_dir, f)

                if gen_count < self.generation:
                    # We only need to keep one old request file around to preserve the gen count
                    try_remove(request_filename)
                elif gen_count > self.generation:
                    is_message_processed = True
                    self.generation = gen_count
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
                        continue

                    response_bytes = self.handler.process_request(request_bytes)
                    try:
                        if response_bytes:
                            write_file_with_subdirs(response_filename, response_bytes)
                    except EnvironmentError, e:
                        self.log.error('Unable to write response \"%s\": %s' %
                            (response_filename, str(e)))

            # Don't sleep unless there was no work available
            if not is_message_processed:
                time.sleep(poll_interval)



class SpookyTimeoutError(Exception):
    """This exception is raised when a request timeout is exceeded.  The
    'id' attribute contains the identifier for the request which timed out.
    """

    def __init__(self, value):
        self.id = value

    def __str__(self):
        return 'Timeout exceeded for request id %u.' % self.id



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
        int

            Request identifier, intended for use with check_response() or
            wait_response().

        Raises:
        -------
        EnvironmentError, if the request could not be sent
        """
        next_gen_count = self._get_generation_count() + 1
        msg_filename   = os.path.join(self.request_dir, make_msg_filename(next_gen_count))
        write_file_with_subdirs(msg_filename, request_bytes)
        return next_gen_count


    def check_response(self, request_id):
        """Check whether a response is available for the given request id.

        Note: if a response is successfully retrieved, then the request_id is
        invalidated.  (Request identifiers are not guaranteed to be unique
        across the program lifetime.)

        Parameters:
        -----------
        request_id : int

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
        response_file  = os.path.join(self.response_dir, make_msg_filename(request_id))
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
        return self.wait_response(self.send_request_nowait(request_bytes), timeout)


    def clean_orphan_responses(self):
        """Delete any stale response files.

        In general, response files will fail to be deleted whenever the client does not
        wait for them.  This will occur, for example, if send_request_wait() times out
        or if the client invokes send_request_nowait() without polling check_response()
        until the response is received.

        Returns:
        --------
        list of int

            List of orphaned request ids which were removed.
        """
        result = []
        for (msg, gen_count) in get_messages(self.response_dir):
            try_remove(os.path.join(self.response_dir, msg))
            result.append(gen_count)
        return result


    def _write_request(self, request_bytes):
        """Write a request packet.

        Returns: new generation count for the request.
        """


    def _get_generation_count(self):
        """Get the generation count.

        The client takes responsibility for setting a generation count which
        will be unambiguous for both client and server.  This is why both the
        request and response messages need to be examined.
        """
        return max(get_generation_count(self.request_dir),
                    get_generation_count(self.response_dir))


__all__ = [
    'SpookyServer',
    'SpookyClient'
]


