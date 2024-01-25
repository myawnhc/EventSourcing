#FROM hazelcast:5.3.6-slim
FROM hazelcast/hazelcast:5.3.6-jdk17
ARG HZ_HOME=/opt/hazelcast
#ARG REPO_URL=https://repo1.maven.org/maven2/com/hazelcast

ADD Event/target/eventsourcing-1.0-SNAPSHOT.jar $HZ_HOME/bin/user-lib
ADD Event/target/dependentJars/grpc-connectors-1.0-SNAPSHOT.jar $HZ_HOME/bin/user-lib
ADD Event/target/classes/hazelcast.xml $HZ_HOME/config/hazelcast-docker.xml
ADD Event/target/classes/logging.properties $HZ_HOME/config/logging.properties
