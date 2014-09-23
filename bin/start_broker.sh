set -e

show_help() {
  echo "Usage:";
  echo "  -h, --help: display this message";
  echo "  -d, --kafka_path: The path to the kafka installation (required)";
  echo "  -c, --config: the path of the configuration file";
  exit 1;
}

cfg="server0.properties"

while true ; do
  case "$1" in
    -h|--help)
      show_help ;;
    -d|--kafka-path)
      d=${2} ;
      shift 2 ;;
    -c|--config)
      cfg=${2} ;
      shift 2 ;;
    *)
      break ;
      shift 2 ;;
  esac
done

# make sure the path is defined
if [ ! -d "${d}" ]; then echo "invalid kafka path ${d}" ; exit 1 ; fi

"${d}/bin/kafka-server-start.sh" ${cfg} &
disown

exit 1