# Contributing

Report bugs or suggest changes in [GitHub Issues](https://github.com/keemgdeok/kafka-active-active/issues).

## Report a bug

Include the commit or version, steps to reproduce, expected and actual results, and relevant logs. For failure scenarios, include the seed and where the failure was injected, if available.

## Open a pull request

Run the build with JDK 21.

```bash
./gradlew --no-daemon build
```

For changes to Kafka behavior, run the integration tests with Docker running.

```bash
./gradlew --no-daemon integrationTest
```

Describe what changed and include the commands you ran and their results. Link the issue if there is one.
