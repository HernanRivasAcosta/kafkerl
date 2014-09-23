set -e

show_help() {
  echo "Usage:";
  echo "  -h, --help: display this message";]
  echo "  -c, --config: the path of the configuration file";
  exit 1;
}

cfg="server0.properties"

while true ; do
  case "$1" in
    -h|--help)
      show_help ;;
    -c|--config)
      cfg=${2} ;
      shift 2 ;;
    *)
      break ;
      shift 2 ;;
  esac
done

ps -ax | grep kafka | grep ${cfg} | awk '{print $1}' | xargs kill -15

exit 1