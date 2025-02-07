# Download Ubuntu 24.04 from Microsoft Image Registry
FROM mcr.microsoft.com/mirror/docker/library/ubuntu:24.04

# To build the image, run the following command:
# docker build -t pftnodecontainer .
# docker run  --name remote_server -it -p 58897:22 pftnodeimage -t pftnodecontainer


ARG USER=pftadmin
ARG PEM_KEY=cluster_key.pub
ARG PYTHON_REQS=requirements.txt

RUN apt-get update && apt-get install -y \
    build-essential \
    linux-tools-generic \
    linux-tools-common \
    clang \ 
    curl \
    cmake \
    jq \
    llvm \ 
    screen \
    pkg-config \
    python3 \
    python3-pip\
    python3-virtualenv \
    ca-certificates \ 
    protobuf-compiler \
    net-tools \ 
    libssl-dev \ 
    librocksdb-dev \
    libprotobuf-dev \ 
    openssh-server 

# Get Rust
 RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
# Place Rust in PATH
 ENV PATH="/root/.cargo/bin:${PATH}"
# Install Azure CLI
RUN curl -sL https://aka.ms/InstallAzureCLIDeb | bash


# Create a new user
RUN useradd -ms /bin/bash ${USER}
# Change password
RUN echo pftadmin:password1234 | chpasswd

## Setup SSH (this is not cloud-native, but is necessary to keep consistent with
## the rest of the experiments, which are cloud-native)
## Create ssh directory in home for non-root user
RUN mkdir -p /home/${USER}/.ssh
RUN mkdir -p /var/run/sshd
## Copy the ssh public key in the authorized_keys file.
COPY ${PEM_KEY} /home/${USER}/.ssh/authorized_keys
## change ownership of the key file.
RUN chown -R ${USER}:$GID /home/${USER}/.ssh && \
    chmod 700 /home/${USER}/.ssh && \
    chmod 600 /home/${USER}/.ssh/authorized_keys
## authorize SSH connection with public key 
RUN echo "PubkeyAuthentication yes" > /etc/ssh/sshd_config
## Enable SFTP (necessary for Fabric to work)
RUN echo "Subsystem sftp /usr/lib/openssh/sftp-server" >> /etc/ssh/sshd_config
## Export Port 22 for SSH Connections
EXPOSE 22
## Export Ports For Nodes. TODO(natacha): this is a hack. Currently, opening 50 ports and
## assuming that is enough. 3000 is also currently hardcoded. Will need to fix this. 
EXPOSE 3000-3050

## Start SSH service
# RUN service ssh start

CMD ["/usr/sbin/sshd", "-D"]
