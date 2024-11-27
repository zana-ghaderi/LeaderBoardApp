from confluent_kafka import Consumer
import redis
import json
import time

from Intuit.LeaderBoard.config import KAFKA_SERVER, REDIS_CONFIG
from Intuit.LeaderBoard.database import insert_score, get_top_scores
from Intuit.LeaderBoard.src.kafka_producer import produce_test_scores

# Initialize the Kafka Consumer
consumer = Consumer({
    'bootstrap.servers': KAFKA_SERVER,
    'group.id': 'score-group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})

consumer.subscribe(['game-scores-topic'])
r = redis.Redis(**REDIS_CONFIG)


def process_scores():
    try:
        while True:
            msg = consumer.poll(timeout=1.0)  # Poll for new messages
            if msg is None:
                print("No message received, checking again...")
                continue
            if msg.error():
                print(f"Error: {msg.error()}")
                continue

            try:
                score_data = json.loads(msg.value().decode('utf-8'))
                print(f"Received message: {score_data}")  # Debug print
                player_name = score_data['player_name']
                score = score_data['score']

                # Insert into DB and update cache
                insert_score(player_name, score)
                top_scores = get_top_scores()

                formatted_scores = [
                    {
                        'player_name': name,
                        'score': score,
                        'timestamp': timestamp.isoformat()
                    } for name, score, timestamp in top_scores
                ]

                r.set('top_5_scores', json.dumps(formatted_scores))
                print(f"Processed score for {player_name}: {score}")

            except KeyError as e:
                print(f"Missing key in message: {e}")
            except json.JSONDecodeError:
                print("Failed to decode JSON from message")

            # Debug: Check Redis data
            redis_data = r.get('top_5_scores')
            if redis_data:
                print("Current Top 5 Scores in Redis:", json.loads(redis_data))
            else:
                print("No top scores found in Redis.")

    finally:
        consumer.close()


if __name__ == "__main__":
    produce_test_scores()  # Produce test scores before processing
    time.sleep(10)  # Wait a bit to ensure all messages are processed
    process_scores()
