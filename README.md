# Kafka Listener Framework

A reusable Python framework for building Kafka-based microservices with message routing and filtering.

## Quick Start

```bash
# 1. Create virtual environment
python -m venv .venv

# 2. Activate virtual environment
.\.venv\Scripts\Activate.ps1

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run listener
python main.py

# 5. Send dummy message
# Open producer.py file under kafka_framework, update code towards end to send correct message
python .\kafka_framework\producer.py
```

## Project Structure

```
kafka-listener-project/
+-- kafka_framework/         # Reusable framework
�   +-- consumer.py         # Message consumer
�   +-- producer.py         # Message producer
+-- services/               # Your services
�   +-- summerization_service/
�   +-- recommendation_feedback_service/
+-- Dockerfile             # Container image
```

## Creating a New Service

1. Copy a service template
2. Implement your handlers and models
3. Update configuration in main.py to add handler
```python
    listener.register_handler(
        <your_handler_name>(producer, Config.OUTPUT_TOPIC)
    )
```
4. Deploy


