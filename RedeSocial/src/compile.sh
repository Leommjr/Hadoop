#!/bin/bash
export JAVA_HOME=/usr/lib/jvm/default-java
export PATH=${JAVA_HOME}/bin:${PATH}
export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar

hadoop com.sun.tools.javac.Main Extract.java
jar cf extract.jar Extract*.class

