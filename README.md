kafkerl
=======

Apache Kafka high performance producer/consumer for erlang.
Developed thanks to the support and sponsorship of [TigerText](http://www.tigertext.com/).

##Features (aka, why kafkerl?)
 - Fast binary creation/
 - Caching requests to build more optimally compressed multi message TCP packages.
 - Highly concurrent, using @jaynel concurrency tools.
 - Messages are not lost but cached before sending to kafka.
 - Handles server side errors and broker/leadership changes.
 - The message parser can handle the partial messages sent by the kafka server (as detailed [here](https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-FetchAPI)) without having to wait for the server to send all data. Thanks to this, kafkerl can consume millons of messages memory issues. 
 - Flexible API allows consumer of messages to define pids, funs or M:F pairs as callbacks for the received messages.

##Configuration

Kafkerl is configured setting what producers and consumers will be available. Creating multiple producers will not increase performance by itself but it means you can set different caching policies for different use cases. For example, you might want to flush the cache more often for longer messages.

```erlang
{kafkerl, [{gen_server_name, kafkerl_connection},
           {conn_config, [{brokers, [{"localhost", 9092},
                                     {"127.0.0.1", 9092}]},
                          {client_id, kafkerl_test_client},
                          {tcp_options, [{key, value}]}
                          {max_retries, 5},
                          {max_metadata_retries, -1}]},
           {topics, [test1, test2, test3, test4]}]}.
```
 - **tcp_options** are passed to the socket when connecting, the available options are documented [here](http://erlang.org/doc/man/gen_tcp.html#type-connect_option).