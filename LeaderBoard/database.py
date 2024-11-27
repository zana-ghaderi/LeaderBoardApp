from datetime import datetime

import psycopg2

from Intuit.LeaderBoard.config import DB_CONFIG

# Establish a connection to the PostgreSQL database
conn = psycopg2.connect(**DB_CONFIG)

def create_table():
    with conn.cursor() as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS player_scores_2 (
                id SERIAL PRIMARY KEY,
                player_name VARCHAR(255) NOT NULL,
                score INTEGER NOT NULL,
                timestamp TIMESTAMP NOT NULL
            )
        """)
        conn.commit()
def insert_score(player_name, score):
    with conn.cursor() as cursor:
        cursor.execute(
            "INSERT INTO player_scores_2 (player_name, score, timestamp) VALUES (%s, %s, %s)",
            (player_name, score, datetime.now())
        )
        conn.commit()

def get_top_scores(limit=5):
    with conn.cursor() as cursor:
        cursor.execute("""
            SELECT player_name, score, timestamp 
            FROM player_scores_2 
            ORDER BY score DESC, timestamp ASC 
            LIMIT %s
        """, (limit,))
        return cursor.fetchall()

create_table()