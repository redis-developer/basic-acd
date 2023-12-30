FROM haproxytech/haproxy-ubuntu:latest
USER root
RUN apt-get update
RUN apt-get install keepalived -y
RUN apt-get install psmisc -y
CMD service keepalived start; haproxy -f /usr/local/etc/haproxy/haproxy.cfg