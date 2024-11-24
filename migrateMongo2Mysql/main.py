import subprocess
import time


def run_script(script_name):
    """Run a Python script as a subprocess."""
    return subprocess.Popen(['python', script_name])


if __name__ == "__main__":
    try:
        print("Starting Kafka Consumer...")
        kafka_consumer = run_script('kafkaConfig.py')

        # Add a delay if necessary to ensure Kafka consumer starts first
        time.sleep(5)

        print("Starting Migration Script...")
        migration = run_script('migrate.py')

        print("All services are running. Press Ctrl+C to stop.")

        # Keep the main script running
        kafka_consumer.wait()
        migration.wait()
    except KeyboardInterrupt:
        print("Stopping all services...")
        kafka_consumer.terminate()
        migration.terminate()
        print("All services stopped.")
