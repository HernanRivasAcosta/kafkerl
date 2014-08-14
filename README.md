kafkerl v0.9.0
==============

Apache Kafka high performance producer/consumer for erlang.
Developed thanks to the support and sponsorship of [TigerText](http://www.tigertext.com/).

##Features (aka, why kafkerl?)
 - Fast binary creation
 - Caching requests to build more optimally compressed multi message TCP packages.
 - Highly concurrent, using @jaynel concurrency tools (v1.0 onwards)
 - Messages are not lost but cached before sending to kafka.
 - Handles server side errors and broker/leadership changes (v1.0 onwards)
 - The message parser can handle the partial messages sent by the kafka server (as detailed [here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI)) without having to wait for the server to send all data. Thanks to this, kafkerl can consume millons of messages memory issues (v1.0 onwards)
 - Flexible API allows consumer of messages to define pids, funs or M:F pairs as callbacks for the received messages (v1.0 onwards)
