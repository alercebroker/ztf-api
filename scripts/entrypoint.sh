#!/bin/bash
echo "Starting ZTF Postgres API"
gunicorn --bind $APP_BIND:$APP_PORT --workers $APP_WORKERS deploy:app
