import json
import logging
import dash_auth
from dash_kv_pair import dash_key_value_pair

# appConfig = None
# try:
#     f = open("local/config.json", "r")
#     appConfig = json.load(f)

# except Exception as e:
#     logging.error('user mng db error. error reason: %s' % e)

# def addAuth(app):
#     dash_auth.BasicAuth(
#         app,
#         appConfig.get('users', {}))

def addAuth(app):
    dash_auth.BasicAuth(
        app,
        dash_key_value_pair)