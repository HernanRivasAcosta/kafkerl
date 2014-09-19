kafkerl v1.0.0
==============

Apache Kafka 0.8 high performance producer for erlang.
Developed thanks to the support and sponsorship of [TigerText](http://www.tigertext.com/).

##Features (aka, why kafkerl?)
 - Fast binary creation.
 - Caching requests to build more optimally compressed multi message TCP packages.
 - Highly concurrent, using @jaynel concurrency tools.
 - Messages are not lost but cached before sending to kafka.
 - Handles server side errors and broker/leadership changes.
 - Flexible API allows consumer of messages to define pids, funs or M:F pairs as callbacks for the received messages.

##Missing features (aka, what I am working on but haven't finished yet)
 - Though the library can parse kafka messages, the consumers are not implemented in this version.
 - There is no communication with Zookeeper.
 - Tests suites.


Special thanks to @nitzanharel who not only found some really nasty bugs and helped me understand the subtelties of kafka's design and to the rest [TigerText](http://www.tigertext.com/) team and their support and code reviews.