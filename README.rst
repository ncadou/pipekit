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
    pipe.send('Hello world')

    print(pipe.receive())

    # Pipes are iterables, too:
    for msg in pipe:
        dosomething(msg)

    # Need a 0mq-based pipe instead?
    from pipekit import ZMQPipe

    my0mqpipe = ZMQPipe('my-0mq-pipe', address='tcp://*:5555')

.. _Queue: https://docs.python.org/3.5/library/queue.html
.. _0mq: http://zeromq.org/
