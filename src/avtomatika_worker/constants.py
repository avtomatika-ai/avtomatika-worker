"""
Centralized constants for the Avtomatika protocol (Worker SDK).
These should match the constants in the core `avtomatika` package.
"""

# --- Auth Headers ---
AUTH_HEADER_CLIENT = "X-Avtomatika-Token"
AUTH_HEADER_WORKER = "X-Worker-Token"

# --- Error Codes ---
ERROR_CODE_TRANSIENT = "TRANSIENT_ERROR"
ERROR_CODE_PERMANENT = "PERMANENT_ERROR"
ERROR_CODE_INVALID_INPUT = "INVALID_INPUT_ERROR"

# --- Task Statuses ---
TASK_STATUS_SUCCESS = "success"
TASK_STATUS_FAILURE = "failure"
TASK_STATUS_CANCELLED = "cancelled"
TASK_STATUS_NEEDS_REVIEW = "needs_review"  # Example of a common custom status

# --- Commands (WebSocket) ---
COMMAND_CANCEL_TASK = "cancel_task"
