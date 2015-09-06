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

echo "Build comleted, running $assignment_name"
case assignment_name in
  TitleCount)
    hadoop jar TitleCount.jar TitleCount -D stopwords=/mp2/misc/stopwords.txt -D delimeters=/mp2/misc/delimiters.txt /mp2/titles /mp2/A-output
    ;;
esac
