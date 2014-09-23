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

"${d}/bin/kafka-topics.sh" --zookeeper localhost:2181 --create --topic test1 --partitions 3 --replication-factor 3
"${d}/bin/kafka-topics.sh" --zookeeper localhost:2181 --create --topic test2 --partitions 3 --replication-factor 3
"${d}/bin/kafka-topics.sh" --zookeeper localhost:2181 --create --topic test3 --partitions 4 --replication-factor 3