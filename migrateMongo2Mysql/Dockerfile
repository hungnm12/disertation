FROM python:3.9-slim-buster

WORKDIR /app

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc

# Copy the requirements file and install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Expose the Kafka consumer port (if needed)
EXPOSE 8080

# Set the command to run your Kafka consumer
CMD ["python", "kafka_consumer.py"]