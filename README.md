kafkerl v1.0.2
==============
[![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/HernanRivasAcosta/kafkerl?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

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



Special thanks to [@nitzanharel](https://github.com/nitzanharel) who found some really nasty bugs and helped me understand the subtleties of kafka's design and to the rest of the [TigerText](http://www.tigertext.com/) team for their support and code reviews.
