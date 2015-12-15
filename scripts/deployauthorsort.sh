export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${JAVA_HOME}/lib/tools.jar:/home/hduser/users/eric021/authorsort/src
cd ~/Application/hadoop-2.7.1
bin/hadoop com.sun.tools.javac.Main /home/hduser/users/eric021/authorsort/src/edu/if4031/AuthorSort.java
cd ~/users/eric021/authorsort/src
jar cfe authorsort.jar edu.if4031.AuthorSort edu/if4031/*.class
mv authorsort.jar ../jar
cd ~/Application/hadoop-2.7.1
cp ~/users/eric021/authorsort/jar/authorsort.jar .
