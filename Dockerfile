FROM ubuntu:18.04

RUN apt-get update -y
RUN apt-get install -y apt-utils python3.7 python3-dev python3-setuptools python3-pip
RUN apt-get install -y postgresql-client libxml2-dev libxslt-dev git
RUN apt-get clean

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
