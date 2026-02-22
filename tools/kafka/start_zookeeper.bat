@echo off

cd /d C:\kafka

echo Starting Zookeeper...
bin\windows\zookeeper-server-start.bat config\zookeeper.properties
