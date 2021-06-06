FROM ubuntu:latest

ARG shared_workspace=/opt/workspace
ARG openjdk_version="14"

# Install Python, Java and other useful components
RUN mkdir -p ${shared_workspace}/data && \
    apt-get -y update && \
    apt-get install --no-install-recommends -y \
    python \
    curl \
    tini \
    wget \
    "openjdk-${openjdk_version}-jre-headless" \
    ca-certificates-java && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV SHARED_WORKSPACE=${shared_workspace} \
    JAVA_HOME=/usr/lib/jvm/java-${openjdk_version}-openjdk-amd64 \
    PATH=$JAVA_HOME/bin:$PATH

VOLUME ${shared_workspace}

CMD ["bash"]