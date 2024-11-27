from datetime import datetime, timedelta


from confluent_kafka import Producer
import json
import random

from Intuit.LeaderBoard.config import KAFKA_SERVER

producer = Producer({'bootstrap.servers': KAFKA_SERVER})

def publish_score(player_name, score, timestamp):
    message = json.dumps({
        "player_name": player_name,
        "score": score,
        "timestamp": timestamp.isoformat()
    }).encode('utf-8')
    producer.produce('game-scores-topic', message)
    producer.flush()

def produce_test_scores(num_players=30):
    # Create a base timestamp
    base_timestamp = datetime.now() - timedelta(days=1)

    # Create some predefined high scores to ensure ties
    high_scores = [
        ("HighScorer_1", 1000, base_timestamp),
        ("HighScorer_2", 1000, base_timestamp + timedelta(minutes=5)),
        ("HighScorer_3", 1000, base_timestamp + timedelta(minutes=10)),
    ]

    # Publish predefined high scores
    for player, score, timestamp in high_scores:
        publish_score(player, score, timestamp)
        print(f"Published score for {player}: {score} at {timestamp}")

    # Generate random scores for the rest of the players
    for i in range(num_players - len(high_scores)):
        player_name = f"Player_{i + 1}"
        score = random.randint(1, 999)  # Random score between 1 and 999
        timestamp = base_timestamp + timedelta(minutes=random.randint(1, 1440))  # Random time within 24 hours
        publish_score(player_name, score, timestamp)
        print(f"Published score for {player_name}: {score} at {timestamp}")