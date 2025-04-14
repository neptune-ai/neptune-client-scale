# Input validation
MAX_RUN_ID_LENGTH = 128
MAX_EXPERIMENT_NAME_LENGTH = 730

# Threads
SYNC_THREAD_SLEEP_TIME = 0.5
STATUS_TRACKING_THREAD_SLEEP_TIME = 1
ERRORS_MONITOR_THREAD_SLEEP_TIME = 0.1
SYNC_PROCESS_SLEEP_TIME = 0.2
LAG_TRACKER_THREAD_SLEEP_TIME = 1
PROCESS_SUPERVISOR_THREAD_SLEEP_TIME = 0.5

# Networking
# This timeout is applied to each networking call individually: connect, write, and read. Thus, it is
# not a timeout for an entire API call.
HTTP_CLIENT_NETWORKING_TIMEOUT = 30
# We allow this many seconds for a single HTTP request, including retries.
HTTP_REQUEST_MAX_TIME_SECONDS = 360

# User facing
SHUTDOWN_TIMEOUT = 60  # 1 minute
MINIMAL_WAIT_FOR_PUT_SLEEP_TIME = 10
MINIMAL_WAIT_FOR_ACK_SLEEP_TIME = 10
STOP_MESSAGE_FREQUENCY = 5
LAG_TRACKER_TIMEOUT = 1
OPERATION_REPOSITORY_POLL_SLEEP_TIME = 1

# Status tracking
MAX_REQUESTS_STATUS_BATCH_SIZE = 1000

# Operations
MAX_SINGLE_OPERATION_SIZE_BYTES = 2 * 1024 * 1024  # 2MB
MAX_REQUEST_SIZE_BYTES = 16 * 1024 * 1024  # 16MB
OPERATION_REPOSITORY_TIMEOUT = 60  # 1 minute

# Files
# Max total length of all fields in a FileRef: storage path, attribute path, mime type
MAX_PROTO_FILE_REF_FIELDS_TOTAL_LENGTH = 3800
# Max length of destination path in file storage backend
MAX_FILE_DESTINATION_LENGTH = 800
