#!/bin/sh

rmiregistry 1099 &
java -cp .:../src/protobuf-java-2.5.0.jar NameNode
