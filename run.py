from src.pipelines.iqfeed import historical

historical("127.0.0.1", 9100, "20240101", "20240301", "60", ["AAPL", "GOOGL"])