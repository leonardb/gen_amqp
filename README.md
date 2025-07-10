# gen_amqp

`gen_amqp` is an Erlang library that provides a generic behavior for interacting with RabbitMQ. It abstracts the details of connection management, channel handling, queue declaration, message publishing, and consumption, offering a simple and consistent interface for Erlang applications.

Why use `gen_amqp`?

- **Simplicity:** Provides a high-level API for common AMQP operations, reducing boilerplate code.
- **Reliability:** Manages connections and channels, handling reconnections and error scenarios gracefully.
- **Integration:** Designed to fit naturally into Erlang/OTP applications, leveraging OTP principles and patterns.
- **Testability:** Facilitates testing by providing clear abstractions and interfaces.

## Queueing and Message Buffering

When the connection to the AMQP broker is down, `gen_amqp` can buffer outgoing messages according to the configured buffering strategy. This allows your application to continue sending messages even during temporary outages, with delivery deferred until the connection is restored.

### Buffering Types

- **none**  
  No buffering is performed. Attempts to publish while disconnected will immediately return an error.  
  *Pros*: Simple, no memory or resource overhead.  
  *Cons*: Messages are lost if the connection is unavailable.

- **queue**  
  Messages are buffered in an in-memory FIFO queue while the connection is down. When the connection is restored, buffered messages are sent in order.  
  *Pros*: No message loss during short outages, fast recovery.  
  *Cons*: Memory usage grows with the number of buffered messages; messages are lost if the process crashes before reconnection.

- **Mod**  
  A custom buffering strategy, where `Mod` is a module that implements your own buffering logic (for example, using ETS, disk, or a distributed store).  
  The module must export the following functions:

    - `buff_put(Module :: module(), Msg :: any())`  
      Add a message to the buffer.

    - `buff_lock(Module :: module(), Count :: pos_integer())`  
      Lock and retrieve up to `Count` messages for delivery. Returns `{ok, list(any())}`.

    - `buff_unlock(Id :: any())`  
      Unlocks the buffer after delivery attempt.

    - `buff_delete(Id :: any())`
      Remove all locked messages from the buffer after successful delivery.

  You can refer to the sample implementation in [`test/buffer.erl`](test/buffer.erl), which uses ETS for in-memory buffering.

  You own implementation does not have to use integer based keys like the example.

  *Pros*: Full control over buffering, can implement persistence, custom ordering, or distributed buffering.  
  *Cons*: Requires additional implementation and testing; performance and reliability depend on your code.

### Behavior on Reconnection

Once the connection is restored, `gen_amqp` automatically flushes the buffer (if enabled), delivering all queued messages in order. If buffering is set to `none`, only new messages sent after reconnection will be delivered.

Choose the buffering strategy that best fits your application's reliability, durability, and performance needs.

