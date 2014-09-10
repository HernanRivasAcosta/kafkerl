ps ax | grep -i 'zookeeper' | grep -v grep | awk '{print $1}' | xargs kill -9
ps ax | grep -i 'kafka\.Kafka' | grep java | grep -v grep | awk '{print $1}' | xargs kill -9