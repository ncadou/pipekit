Pipekit
=======

Pipekit_ is a flow-based_ programming toolkit, with a control layer.

.. _Pipekit: https://github.com/ncadou/pipekit
.. _flow-based: https://en.wikipedia.org/wiki/Flow-based_programming


Quick start
===========

Pipekit connects message processors using pipes. Pipes are just a thin layer on
top of Queue_ objects and 0mq_ sockets, wrapping them under a common API. The
basic idea behind this abstraction is the possibility to transparently replace
a pipe implementation with another one, with no code change needed in the
producers/consumers.

Pipes simply have an input and an output channel; creating and using them is
pretty straightforward:

.. code:: python

    from pipekit import ThreadPipe

    # Pipes need to be given a name
    mypipe = ThreadPipe('my-pipe')
    mypipe.send('Hello world')

    print(mypipe.receive())
    # Hello world

Pipes are iterables, too:

.. code:: python

    for msg in mypipe:
        dosomething(msg)

Need a 0mq-based pipe instead?

.. code:: python

    from pipekit import ZMQPipe

    my0mqpipe = ZMQPipe('my-0mq-pipe', address='tcp://*:5555')

Alternatively:

.. code:: python

    from pipekit import Pipe

    my0mqpipe = Pipe('my-0mq-pipe', impl='zmq', address='tcp://*:5555')
    print(my0mqpipe)
    # <pipekit.ZMQPipe object at 0x7fe...>

.. _Queue: https://docs.python.org/3/library/queue.html
.. _0mq: http://zeromq.org/
