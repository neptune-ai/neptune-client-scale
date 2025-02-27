# Input validation
MAX_RUN_ID_LENGTH = 128
MAX_EXPERIMENT_NAME_LENGTH = 730

# Operations queue
MAX_BATCH_SIZE = 100000
MAX_QUEUE_SIZE = 1000000
MAX_MULTIPROCESSING_QUEUE_SIZE = 32767
MAX_QUEUE_ELEMENT_SIZE = 1024 * 1024  # 1MB
# Wait up to this many seconds for incoming operations before submitting a batch
BATCH_WAIT_TIME_SECONDS = 1

# Threads
SYNC_THREAD_SLEEP_TIME = 0.5
STATUS_TRACKING_THREAD_SLEEP_TIME = 1
ERRORS_MONITOR_THREAD_SLEEP_TIME = 0.1
SYNC_PROCESS_SLEEP_TIME = 1
LAG_TRACKER_THREAD_SLEEP_TIME = 1

# Networking
REQUEST_TIMEOUT = 30
MAX_REQUEST_RETRY_SECONDS = 60

# User facing
SHUTDOWN_TIMEOUT = 60  # 1 minute
MINIMAL_WAIT_FOR_PUT_SLEEP_TIME = 10
MINIMAL_WAIT_FOR_ACK_SLEEP_TIME = 10
STOP_MESSAGE_FREQUENCY = 5
LAG_TRACKER_TIMEOUT = 1

# Status tracking
MAX_REQUESTS_STATUS_BATCH_SIZE = 1000
