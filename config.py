from airflow.models import Variable

config = {
    'iqfeed': {
        'host': Variable.get('iqfeed_host', 'host.docker.internal'),
        'port': int(Variable.get('iqfeed_port', '9100')),
    },
    'questdb': {
        'host': Variable.get('questdb_host', 'questdb'),
        'port': int(Variable.get('questdb_port', '8812')),
        'username': Variable.get('questdb_username', 'admin'),
        'password': Variable.get('questdb_password', 'quest'),
        'database': Variable.get('questdb_database', 'qdb'),
    },
    'data': {
        'tickers': ['AAPL','MSFT','GOOGL','AMZN','TSLA'],
        'interval': '60',
        'days_look_back': 30,
    }
}