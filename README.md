# Flink Realtime for Data Engineers

## Setup

Run `./scripts/repo-setup.sh`.

### IntelliJ

[Configure a JAR artifact](https://www.jetbrains.com/help/idea/compiling-applications.html#package_into_jar) for the
project.
Do not use a main class: you'll supply that later. For convenience, be sure to check "Include in project build".

## Environment: Docker Compose

### Startup

`docker compose up -d`

### Teardown

`docker compose down`

## Running

1. [Rebuild the project](https://www.jetbrains.com/help/idea/compiling-applications.html#rebuild_project).
1. Run the chapter/class you want to
   demo: `flink run --class com.flinklearn.realtime.<chapter>.<class> ./out/artifacts/RealtimeDataEngg_jar/RealtimeDataEngg.jar`.
1. Follow along with logs/STDOUT: `docker compose logs --follow taskmanager`