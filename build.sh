#!/bin/bash

echo -n "Assignment: "
read assignment_name

echo "Building $assignment_name"

echo "Cleaning builds..."
rm -rf ./build/* ./${assignment_name}.jar


echo "Starting build"
export HADOOP_CLASSPATH=$JAVA_HOME/lib/tools.jar
hadoop com.sun.tools.javac.Main ${assignment_name}.java -d build
jar -cvf ${assignment_name}.jar -C build/ ./

echo "Build completed"
