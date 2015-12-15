export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${JAVA_HOME}/lib/tools.jar:/home/hduser/users/eric021/authorcount/src
cd ~/Application/hadoop-2.7.1
bin/hadoop com.sun.tools.javac.Main /home/hduser/users/eric021/authorcount/src/edu/if4031/AuthorCount.java
cd ~/users/eric021/authorcount/src
jar cfe authorcount.jar edu.if4031.AuthorCount edu/if4031/*.class
mv authorcount.jar ../jar
cd ~/Application/hadoop-2.7.1
cp ~/users/eric021/authorcount/jar/authorcount.jar .
