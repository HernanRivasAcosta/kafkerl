kafkerl
=======

Apache Kafka high performance producer/consumer for erlang

* Features (aka, why kafkerl?)
***note that not all of this are implemented***
 - Fast binary creation
 - Caching requests to build more optimally compressed multi message TCP packages
 - Multiple simultaneous TCP connections to Kafka servers allowing for different cache policies depending on the requirements, for example, flush instantly on a producer designed for large yet sporadic messages but cache N messages before sending when multiple small messages are expected.
 - Highly concurrent using @jaynel concurrency tools.
 - Never lose a message! Different configurable strategies to deal with possible downtime of the kafka server ensures that those messages are saved to disk or sent somewhere else.

* Configuration

Kafkerl is configured setting what producers and consumers will be available. Creating multiple producers will not increase performance by itself but it means you can set different caching policies for different use cases. For example, you might want to flush the cache more often for longer messages.

```erlang
{kafkerl, [{start,
            [{producer,
              [{name, kafkerl_producer_short_msg},
               {config, [{client_id, <<"sample-producer">>}]},
               {conn_config, [{host, "localhost"},
                              {port, 9092},
                              {tcp_options, []},
                              {max_retries, -1}, % use -1 for unlimited retries after a disconnection
                              {retry_interval, 1000}] % the interval between reconnection attempts
                              }]}]}]}
```