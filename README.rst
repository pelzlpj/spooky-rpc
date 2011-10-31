**spooky-rpc** is a simple client/server framework for carrying out remote procedure calls using a
filesystem transport.  It may prove useful in restricted network environments where multiple PCs
have access to a shared network filesystem, but have no useful communication link otherwise.

Overview
========

The `spooky_rpc module`_ is
well-documented with Python docstrings, which should be considered the primary documentation.  This
is just an overview.

.. _spooky_rpc module: http://github.com/pelzlpj/spooky-rpc/blob/master/spooky_rpc.py

**spooky-rpc** abstracts away most of the transport-layer details.  Your job is to define the
following:

    - *A binary request/response protocol.*  **spooky-rpc** just moves blobs of bytes back
      and forth; you need to write the code for serializing and deserializing your request and
      response data structures.

    - *A server-side request handler.*  **spooky-rpc** will invoke your handler whenever a new
      request comes in, and deliver the handler's response.

    - *Client code*, which invokes **spooky-rpc** methods to send requests and wait for
      responses.

To create a server, all you need to do is inherit from ``spooky_rpc.BinaryRequestHandler`` and
implement its interface, then instantiate a ``spooky_rpc.Server`` which uses your handler.  The
client-side code is fairly obvious; just invoke ``spooky_rpc.Client`` methods as necessary to send
requests to the server and poll for arrival of responses.

The unit tests in spooky_rpc.py_ provide a small example of usage.

.. _spooky_rpc.py: http://github.com/pelzlpj/spooky-rpc/blob/master/spooky_rpc.py


Things to Watch Out For
=======================

In order to facilitate request pipelining, ``spooky_rpc.Server`` uses the multiprocessing_ module
to hand off requests to a pool of child processes.  Consequently, the implementation of
``spooky_rpc.BinaryRequestHandler`` cannot assume that the process state will be maintained across
multiple calls to the handler method.  If you need a stateful server, you will have to store that
state outside of the process (e.g. on disk).

.. _multiprocessing: http://docs.python.org/library/multiprocessing.html


License
=======

::

    spooky_rpc

    Copyright (c) 2011 Paul Pelzl
    All rights reserved.

    Redistribution and use in source and binary forms, with or without
    modification, are permitted provided that the following conditions are met:

        Redistributions of source code must retain the above copyright notice, this
        list of conditions and the following disclaimer.

        Redistributions in binary form must reproduce the above copyright notice,
        this list of conditions and the following disclaimer in the documentation
        and/or other materials provided with the distribution.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
    AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
    IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
    DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
    FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
    DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
    SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
    CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
    OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
    OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


Contact Info
============

**spooky-rpc** is `hosted on github <http://github.com/pelzlpj/spooky-rpc>`_.  The author
can be contacted at <``pelzlpj at gmail dot com``>.

