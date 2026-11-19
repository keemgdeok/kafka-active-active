# Released SDK check

Run from the checkout with JDK 21 and Docker Compose. The two brokers use local ports 19092 and 29092.

```sh
scripts/verify-released-sdk.sh 0.1.0
scripts/verify-released-sdk.sh 0.1.0 2000
```

The first argument selects a Maven Central release. The second sets the number of events sent before the outage. It defaults to 24. The standalone Gradle app uses a fresh cache for each run. The **Released SDK check** action also accepts a version and runs the 24-event scenario.

The app sends events to both partitions and keeps the consumer on B during the A outage. It checks commits made on B before restarting A, waits for replication, repairs completion history, and switches the group to A. It then checks that A filters the completed IDs and delivers a new event. A native log audit compares IDs, keys, partitions, and payloads on both brokers. Separate checks exercise the repair and completion index limits with a four-entry budget.

Each run writes `events.jsonl`, `environment.txt`, and application and broker logs under `build/reports/released-sdk/`. The normal publish phase uses 256-byte payloads and a window of 16 requests. Its latency includes the SDK response and its throughput is the number of responses per second in that phase. The B-to-A probe measures from the start of the send until the app observes the replication worker's copied count increase. Restart, backlog recovery, repair scan, completion repair, and replay also have measured intervals. Index bytes are estimates from the SDK. Use the same environment and count when comparing runs.
