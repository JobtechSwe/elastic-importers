FROM ubuntu:18.04

RUN apt-get update

# Install packages to allow apt to use a repository over HTTPS:
RUN apt-get install -y apt-transport-https ca-certificates

RUN apt-get install -y apt-utils python3.7 python3-dev python3-setuptools python3-pip
RUN apt-get install -y postgresql-client libxml2-dev libxslt-dev git curl
RUN apt-get clean

# Add Docker’s official GPG key:
RUN curl -fsSL https://download.docker.com/linux/ubuntu/gpg | apt-key add -

# Verify that you now have the key with the fingerprint, by searching for the last 8 characters of the fingerprint.
RUN apt-key fingerprint 0EBFCD88 | grep "E2D8 8D81 803C 0EBF CD88" || exit 1

COPY . /app

WORKDIR /app
RUN python3 -m pip install -r requirements.txt
RUN python3 setup.py install
# runs unit tests with @pytest.mark.unit annotation only
RUN python3 -m pytest -svv -m unit tests/

# show commit info
RUN git log -1

WORKDIR /
RUN rm -fr /app


USER 10000
