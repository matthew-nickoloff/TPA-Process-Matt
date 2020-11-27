import os


db_credentials = {
    'username': os.getenv("AWS_REDSHIFT_USERNAME"),
    'password': os.getenv("AWS_REDSHIFT_PASSWORD")
}


aws_credentials = {
    'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID'),
    'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY')
}
