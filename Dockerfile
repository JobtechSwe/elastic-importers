FROM ubuntu:18.10

# Install packages to allow apt to use a repository over HTTPS:
RUN apt-get update && apt-get install -yq --no-install-recommends --fix-missing \
    apt-transport-https \
    ca-certificates \
    python3.7 \
    python3-dev \
    python3-setuptools \
    python3-pip \
    postgresql-client \
    libxml2-dev \
    libxslt-dev \
    git \
    curl \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN timedatectl set-timezone Europe/Stockholm
RUN timedatectl

# Add Docker’s official GPG key:
# RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -

# Verify that you now have the key with the fingerprint, by searching for the last 8 characters of the fingerprint.
# RUN apt-key fingerprint 0EBFCD88 | grep "E2D8 8D81 803C 0EBF CD88" || exit 1

COPY . /app

WORKDIR /app

# runs unit tests with @pytest.mark.unit annotation only
RUN python3 -m pip install -r requirements.txt && \
    python3 setup.py install && \
    python3 -m pytest -s -m unit tests/


WORKDIR /
RUN rm -frv /app


USER 10000
