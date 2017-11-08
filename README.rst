Pipekit
=======

Pipekit is a flow-based programming toolkit, with a control layer.

Quick start
===========

Pipekit connects message processors using pipes. Pipes are just a thin layer on
top of Queue objects and 0mq sockets, with a common API.

Pipes simply have an input and an output channel; creating and using a pipe is
pretty straightforward:

.. code:: python

    from pipekit import ThreadPipe

    mypipe = ThreadPipe('my-pipe')
    pipe.send('Hello world')

    print(pipe.receive())

    # Pipes are iterable, too:
    for msg in pipe:
        dosomething(msg)

    # Want a 0mq-based pipe instead?
    import zmq
    from pipekit import ZMQPipe

    context = zmq.Context()
    my0mqpipe = ZMQPipe('my-0mq-pipe', context, 'tcp://*:5555')
