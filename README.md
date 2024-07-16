# Flink Realtime for Data Engineers

## Setup

`./scripts/repo-setup.sh`

## Environment: Docker Compose

### Startup

`docker compose up -d`

### Teardown

`docker compose down`

## Running

1. [Configure a JAR artifact](https://www.jetbrains.com/help/idea/compiling-applications.html#package_into_jar),
   using the demo you'd like to run as the main class. For convenience, be sure to check
   "Include in project build".
2. [Rebuild the project](https://www.jetbrains.com/help/idea/compiling-applications.html#rebuild_project).
3. Run `flink run ./out/artifacts/RealtimeDataEngg_jar/RealtimeDataEngg.jar`.