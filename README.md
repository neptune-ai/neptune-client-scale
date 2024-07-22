# Simplified Python Client

It’s an extension of the `NeptuneIngestion` that could be used for data ingestion without access to Kafka and communicate with Neptune 3.0 instances over REST API. The target audience is potential clients who want to test the new application and it’s perforamcne at a low introduction cost. In addition to `NeptuneIngestion` it introduces support for error handling and waiting for operations to be fully processed by the server.

### Local Environment
Create new virtual environment and run `pip install -r dev_requirements.txt`
