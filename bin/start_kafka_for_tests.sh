set -e

show_help() {
  echo "Usage:";
  echo "  -h, --help: display this message";
  echo "  -d, --kafka_path: The path to the kafka installation (required)";
  exit 1;
}

while true ; do
  case "$1" in
    -h|--help)
      show_help ;;
    -d|--kafka-path)
      d=${2} ;
      shift 2 ;;
    *)
      break ;
      shift 2 ;;
  esac
done

# make sure the path is defined
if [ ! -d "${d}" ]; then echo "invalid kafka path ${d}" ; exit 1 ; fi

echo cleaning up
"./clear_kafkerl_test_topics.sh"
sleep 1
echo starting zookeeper
"./start_zk.sh" -d ${d} &
disown
sleep 5
echo starting first broker
"./start_broker.sh" -d ${d} -c server0.properties &
disown
sleep 3
echo starting second broker
"./start_broker.sh" -d ${d} -c server1.properties &
disown
sleep 3
echo starting third broker
"./start_broker.sh" -d ${d} -c server2.properties &
disown
sleep 5
echo creating topics
"./create_test_topics.sh" -d ${d}