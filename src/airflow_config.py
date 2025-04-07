from airflow.models import Variable

config = {
    "iqfeed": {
        "host": Variable.get("iqfeed_host", "host.docker.internal"),
        "port": int(Variable.get("iqfeed_port", "9100")),
    },
}
