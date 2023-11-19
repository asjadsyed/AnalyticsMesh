FROM ubuntu:22.04 AS thrift_build
WORKDIR /opt/hyperloglog-crdt/code/src/
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    thrift-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY ./src/*.thrift ./src/Makefile ./
RUN make

FROM python:3.12.0
WORKDIR /opt/hyperloglog-crdt/code/
COPY ./requirements.txt ./
RUN python3 -m pip install -v -r ./requirements.txt --no-cache-dir --no-input --disable-pip-version-check --no-python-version-warning && rm -rf ./requirements.txt
COPY --from=thrift_build /opt/hyperloglog-crdt/code/src/ ./src/
COPY ./src/ ./src/
WORKDIR /opt/hyperloglog-crdt/data/
VOLUME /opt/hyperloglog-crdt/data/
ENTRYPOINT ["python3", "/opt/hyperloglog-crdt/code/src/main.py"]
CMD []
