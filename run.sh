#!/bin/bash

echo "Starting Docker services..."
docker-compose up -d &> docker-services.log

if ! python3 --version ; then
    echo "python3 is not installed"
    exit 1
fi

echo "Installing dependencies..."

pip3 install pandas
pip3 install requests
pip3 install psycopg2-binary
pip3 install sqlalchemy
pip3 install flask
pip3 install waitress

echo "Running main project..."
python3 main.py