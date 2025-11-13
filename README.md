# queuectl
CLI-based background job queue system

# queuectl - Java CLI job queue

Requirements:
- Java 17+
- Maven

Build:
$ mvn package

Run:
$ java -jar target/queuectl-0.1.0.jar <command>

Examples:

# Enqueue a job
$ java -jar target/queuectl-0.1.0.jar enqueue '{"id":"job1","command":"echo Hello && sleep 1","max_retries":3}'

# Start 2 workers (press ENTER to stop)
$ java -jar target/queuectl-0.1.0.jar worker start --count 2

# See status
$ java -jar target/queuectl-0.1.0.jar status

# List pending jobs
$ java -jar target/queuectl-0.1.0.jar list --state pending

# Show DLQ
$ java -jar target/queuectl-0.1.0.jar dlq list

# Retry DLQ job
$ java -jar target/queuectl-0.1.0.jar dlq retry job1

# Configuration
$ java -jar target/queuectl-0.1.0.jar config set backoff-base 2.5

Notes:
- Jobs are stored in SQLite file `queue.db` in working directory.
- Backoff formula: delay = base ^ attempts (seconds)
