FROM ubuntu:22.04 AS thrift_build
WORKDIR /opt/analytics-mesh/code/
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    thrift-compiler \
    && rm -rf /var/lib/apt/lists/*
COPY ./Makefile ./
COPY ./src/*.thrift ./src/
RUN make

FROM python:3.12.0
WORKDIR /opt/analytics-mesh/code/
COPY ./requirements.txt ./
RUN python3 -m pip install -v -r ./requirements.txt --no-cache-dir --no-input --disable-pip-version-check --no-python-version-warning && rm -rf ./requirements.txt
COPY --from=thrift_build /opt/analytics-mesh/code/src/ ./src/
COPY ./src/ ./src/
WORKDIR /opt/analytics-mesh/data/
VOLUME /opt/analytics-mesh/data/
ENTRYPOINT ["python3", "/opt/analytics-mesh/code/src/main.py"]
CMD []
