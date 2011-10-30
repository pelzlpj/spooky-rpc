
import abc


class RpcHandler(object):
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
        req : string
        
            Request packet, serialized as a byte sequence.

        Returns:
        --------
        string or None

            If the request packet requires a response, then the the return value
            is the byte-serialized response packet; otherwise, None is returned.
        """
        return


__all__ = [
    'RpcHandler'
]

