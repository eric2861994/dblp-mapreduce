export HADOOP_CLASSPATH=${HADOOP_CLASSPATH}:${JAVA_HOME}/lib/tools.jar:/home/hduser/users/eric021/publicationcount/src
cd ~/Application/hadoop-2.7.1
bin/hadoop com.sun.tools.javac.Main /home/hduser/users/eric021/publicationcount/src/edu/if4031/PublicationCount.java
cd ~/users/eric021/publicationcount/src
jar cfe publicationcount.jar edu.if4031.PublicationCount edu/if4031/*.class
mv publicationcount.jar ../jar
cd ~/Application/hadoop-2.7.1
cp ~/users/eric021/publicationcount/jar/publicationcount.jar .
