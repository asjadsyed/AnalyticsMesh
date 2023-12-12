# AnalyticsMesh

## Overview

AnalyticsMesh is a robust and efficient technology for data aggregation and analytics in distributed environments. Unlike traditional centrally coordinated systems, AnalyticsMesh employs a decentralized architecture, providing superior scalability and fault tolerance.

In AnalyticsMesh, nodes operate autonomously, processing and aggregating data, contributing their insights to the collective understanding. This collaborative approach enables the analysis of massive datasets and streams, and ensures data availability even in unpredictable environments.

From tracking mobile app user engagements and digital marketing campaigns across platforms, to monitoring critical services and wireless sensor networks for disruptions, AnalyticsMesh offers a seamless solution for decentralized data aggregation and analytics. Its adaptability and scalability empower users to unlock their distributed data's full potential.

## Technical Details

A distinctive aspect of AnalyticsMesh is its novel enhancement of HyperLogLog, a probabilistic data structure, into a CRDT (Conflict-Free Replicated Data Type). This transformation enables efficient and scalable approximate count-distinct operations, which are essential for big data analytics.

Key features of AnalyticsMesh include:

* **Horizontally Scalable Architecture**: AnalyticsMesh allows for easy scaling by adding more nodes, enhancing capacity and performance without disrupting existing operations.

* **Fault Tolerance**: AnalyticsMesh withstands network partitions, disruptions, delays, and node failures, ensuring continuous data processing without data loss.

* **Strong Eventual Consistency**: Leveraging HyperLogLog's CRDT properties, AnalyticsMesh guarantees eventual consistency in data across all distributed nodes.

* **Tunable Durability and Atomicity**: AnalyticsMesh allows tuning data durability and atomicity guarantees, adapting to various operational requirements.

* **Containerization Support**: Containerization simplifies scalable deployment by providing consistent, isolated environments across platforms.

## Getting Started

### Without Docker

#### Prerequisites:

* Git
* Apache Thrift compiler
* Make
* Python >= 3.12

1. Install prerequisites (Ubuntu/Debian):

```bash
sudo apt update
sudo apt install -y git thrift-compiler make python3.12
```

2. Clone the repository and navigate to the project directory:

```bash
git clone https://github.com/asjadsyed/AnalyticsMesh
cd AnalyticsMesh
```

3. Install Python dependencies:

```bash
python3 -m pip install -r requirements.txt
```

4. Build the project:

```bash
make
```

5. Run the application:

```bash
python3 ./src/main.py --help
```

### With Docker

#### Prerequisites:

* Git
* Docker
* Docker Compose

1. Install prerequisites (Ubuntu/Debian):

```bash
sudo apt update
sudo apt install -y git
sudo snap install docker
```

2. Clone the repository and navigate to the project directory:

```bash
git clone https://github.com/asjadsyed/AnalyticsMesh
cd AnalyticsMesh
```

3. Build Docker containers:

```bash
docker-compose build
```

4. Run the application:

```bash
docker-compose run analytics-mesh --help
```

## License

AnalyticsMesh is licensed under the Apache License 2.0. Refer to the `LICENSE` file for details.
