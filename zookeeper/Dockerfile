FROM confluentinc/cp-zookeeper:latest
USER root
RUN yum update -y && \
    yum install -y nmap-ncat grep && \
    yum clean all
