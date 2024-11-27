import logging
from flask import Flask, jsonify, request
from flask_httpauth import HTTPTokenAuth
import redis
import json
from datetime import datetime

from Intuit.LeaderBoard.config import REDIS_CONFIG

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

app = Flask(__name__)
r = redis.Redis(**REDIS_CONFIG)
auth = HTTPTokenAuth(scheme='Bearer')

# API Tokens configuration
API_TOKENS = {
    'token1': {'username': 'user1', 'role': 'user'},
    'token2': {'username': 'user2', 'role': 'user'},
    'admin_token': {'username': 'admin', 'role': 'admin'}
}


@auth.verify_token
def verify_token(token):
    logger.debug(f"Token received for verification: '{token}'")

    # Manually extract the Authorization header if token is None
    if token is None:
        auth_header = request.headers.get('Authorization')
        logger.debug(f"Authorization header: '{auth_header}'")
        if auth_header and auth_header.startswith('Bearer '):
            token = auth_header[len('Bearer '):]
            logger.debug(f"Extracted token from header: '{token}'")

    if token in API_TOKENS:
        logger.debug(f"Token is valid. Username: {API_TOKENS[token]['username']}")
        return API_TOKENS[token]['username']

    logger.debug("Token is invalid.")
    return None


@app.route('/top-scores', methods=['GET'])
@auth.login_required
def top_scores():
    top_scores = r.get('top_5_scores')
    if top_scores:
        scores = json.loads(top_scores)
        formatted_scores = []
        for score in scores:
            if isinstance(score, list) and len(score) == 3:
                formatted_score = {
                    'player_name': score[0],
                    'score': score[1],
                    'timestamp': datetime.fromisoformat(score[2]).strftime('%Y-%m-%d %H:%M:%S')
                }
                formatted_scores.append(formatted_score)
            elif isinstance(score, dict):
                score['timestamp'] = datetime.fromisoformat(score['timestamp']).strftime('%Y-%m-%d %H:%M:%S')
                formatted_scores.append(score)
        return jsonify(formatted_scores)
    else:
        return jsonify([]), 404

@app.before_request
def log_request_info():
    logger.debug('Headers: %s', request.headers)
    logger.debug('Authorization header: %s', request.headers.get('Authorization'))
    logger.debug('Body: %s', request.get_data())

@auth.error_handler
def auth_error(status):
    logger.error(f"Authentication error: {status}")
    return jsonify(error="Authentication error"), status

@app.errorhandler(401)
def unauthorized(e):
    logger.error(f"Unauthorized access attempt: {e}")
    return jsonify(error="Unauthorized: Authentication is required to access this resource"), 401

@app.errorhandler(Exception)
def handle_exception(e):
    logger.error(f"Unhandled exception: {str(e)}", exc_info=True)
    return jsonify(error="An unexpected error occurred"), 500

if __name__ == '__main__':
    logger.info("Starting the Flask application...")
    app.run(debug=True)
