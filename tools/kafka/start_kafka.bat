@echo off

cd /d C:\kafka

echo Starting Kafka broker...
bin\windows\kafka-server-start.bat config\server.properties
