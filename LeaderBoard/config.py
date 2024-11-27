# Kafka config
KAFKA_SERVER = 'localhost:9092'

# Database config
DB_CONFIG = {
    'database': 'game_db',
    'user': 'zana',
    'password': 'test123',
    'host': 'localhost',
    'port': '5432'
}

REDIS_CONFIG = {
    'host': 'localhost',
    'port': '6379',
    'db': '0'
}

API_TOKENS = {
    'token1': {'username': 'user1', 'role': 'user'},
    'token2': {'username': 'user2', 'role': 'user'},
    'admin_token': {'username': 'admin', 'role': 'admin'}
}
