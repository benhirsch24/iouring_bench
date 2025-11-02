# TODO

**Handle connections closed by peer**

Recv will return 0, need to handle that in Connection::serve() probably to make sure that all subscribers get BYE messages.

**Gracefully fail pub/sub protocol**

Something simple like `ERROR <the error>\r\n`

**Kernel buffers**

I like the idea of a buffer pool struct that manages all the buffers that each connection has a reference to and checks out/owns a buffer from the pool. Will noodle on this more.

**Abstraction around callbacks and buffers**

Keep the core uring event loop steady, create an abstraction around adding callbacks and allocating buffers that are separate.
