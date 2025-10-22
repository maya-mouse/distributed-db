@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

REM Мінімальний Windows .bat для запуску Kafka (без зайвих echo)
REM Задайте шлях до вашого Kafka тут:
set "KAFKA_DIR=D://kafka"
REM Альтернатива (якщо Kafka в папці проекта):
REM set "KAFKA_DIR=C:\Users\mouse\Skrsky\distributedDB\kafka-tkachenko-lab1"

set "BIN_DIR=%KAFKA_DIR%//bin//windows"

REM Перевірки
if not exist "%KAFKA_DIR%" (
  echo ERROR: Kafka не знайдено: "%KAFKA_DIR%"
  pause
  exit /b 1
)

if not exist "%BIN_DIR%" (
  REM Якщо немає bin\windows — спробуємо bin
  if exist "%KAFKA_DIR%\bin" (
    set "BIN_DIR=%KAFKA_DIR%\bin"
  ) else (
    echo ERROR: не знайдено "%KAFKA_DIR%\bin\windows" або "%KAFKA_DIR%\bin"
    pause
    exit /b 1
  )
)

REM Запуск Zookeeper і Kafka у нових вікнах
start "" "%BIN_DIR%//zookeeper-server-start.bat" "%KAFKA_DIR%//config//zookeeper.properties"
timeout /t 5 /nobreak >nul

start "" "%BIN_DIR%//kafka-server-start.bat" "%KAFKA_DIR%//config//server.properties"
timeout /t 10 /nobreak >nul

REM Створити топіки (мінімально)
"%BIN_DIR%//kafka-topics.bat" --create --bootstrap-server localhost:9092 --topic nuclear-main --partitions 3 --replication-factor 1 --config retention.ms=2592000000 --config segment.ms=3600000
"%BIN_DIR%//kafka-topics.bat" --create --bootstrap-server localhost:9092 --topic nuclear-batch-test --partitions 3 --replication-factor 1

for %%C in (none snappy lz4 zstd) do (
  "%BIN_DIR%//kafka-topics.bat" --create --bootstrap-server localhost:9092 --topic nuclear-comp-%%C --partitions 3 --replication-factor 1
)

for %%P in (3 6 9) do (
  "%BIN_DIR%//kafka-topics.bat" --create --bootstrap-server localhost:9092 --topic nuclear-part-%%P --partitions %%P --replication-factor 1
)

REM Перевірка списку (фільтр по nuclear)
"%BIN_DIR%//kafka-topics.bat" --list --bootstrap-server localhost:9092 | findstr /I "nuclear"

REM Короткі performance тести (producer-props як один аргумент у лапках)
"%BIN_DIR%//kafka-producer-perf-test.bat" --topic nuclear-batch-test --num-records 5000 --record-size 256 --throughput -1 --producer-props "bootstrap.servers=localhost:9092 batch.size=1048576 linger.ms=200 compression.type=zstd acks=all"

"%BIN_DIR%//kafka-producer-perf-test.bat" --topic nuclear-comp-zstd --num-records 5000 --record-size 256 --throughput 500 --producer-props "bootstrap.servers=localhost:9092 batch.size=262144 linger.ms=50 compression.type=zstd"

"%BIN_DIR%//kafka-consumer-perf-test.bat" --bootstrap-server localhost:9092 --topic nuclear-batch-test --messages 5000 --threads 1 --show-detailed-stats

REM Налаштування retention (якщо потрібно)
"%BIN_DIR%//kafka-configs.bat" --alter --bootstrap-server localhost:9092 --entity-type topics --entity-name nuclear-main --add-config retention.ms=31536000000

pause

