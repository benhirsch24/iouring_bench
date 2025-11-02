# SIMPLE PUBSUB protocol

Each line is terminated by a carriage return.

Publish:

1. Publisher: PUBLISH channel
2. Server: OK
3. Publisher: message1
4. Publisher: message2
5. Publisher: BYE

Subscribe:

1. Subscriber: SUBSCRIBE channel
2. Server: OK
3. Server: message1
4. Server: message2
5. Subscriber: BYE

Errors:

1. Publisher: PBLISH channel
2. Server: ERROR malformed message

Other ideas:

* Acks? Nah.
* Keep alives - time out a subscriber if a publisher hasn't published in TTL

**Sequence of events**

Should a publisher have to exist before subscribers can join? Can a publisher publish a message to no subscribers? Or are these errors?

For now, both of these are ok. Publishers should be able to join, publish a message, and leave, so subscribers can exist without publishers.
