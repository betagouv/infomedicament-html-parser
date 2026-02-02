# Scalingo Procfile
# Run as a scheduled task using Scalingo Scheduler or one-off container
# https://doc.scalingo.com/platform/app/task-scheduling/scalingo-scheduler

# Parse Notice files (N*.htm)
parse-notices: python -m infomed_html_parser.cli s3 --pattern N

# Parse RCP files (R*.htm)
parse-rcp: python -m infomed_html_parser.cli s3 --pattern R

# Parse both Notice and RCP files
parse-all: python -m infomed_html_parser.cli s3 --pattern NR
