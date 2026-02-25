# Use official Python base image
FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Copy requirements file
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port (Render sets $PORT at runtime)
EXPOSE 10000

# Set environment variables for Flask
ENV FLASK_APP=flask_app.py

# Use gunicorn for production — Render injects $PORT
CMD gunicorn --bind 0.0.0.0:$PORT --workers 2 --threads 2 --timeout 120 flask_app:app
