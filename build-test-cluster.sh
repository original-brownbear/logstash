#!/usr/bin/env bash

./gradlew assembleTarDistribution
cd build
rm -rf logstash1
mkdir logstash1
rm -rf logstash2
mkdir logstash2
rm -rf logstash3
mkdir logstash3

tar -zxf logstash-7.0.0-alpha1-SNAPSHOT.tar.gz -C ./logstash1 --strip 1
tar -zxf logstash-7.0.0-alpha1-SNAPSHOT.tar.gz -C ./logstash2 --strip 1
tar -zxf logstash-7.0.0-alpha1-SNAPSHOT.tar.gz -C ./logstash3 --strip 1
