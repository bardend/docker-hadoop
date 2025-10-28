# Template Hadoop-Spark

This project is a **template for Hadoop-Spark and HDFS**, designed to inject PySpark jobs directly into running containers.

You can change the system configuration as follows:

- **Memory, Cores, and Instances** for the worker and master in `docker-compose-hadoop-spark.yml`.
- **Directories, Ports, and Hostnames** in `.env`.

> 💡 **Note:** I recommended **not to change** the `.env`.  
> For a detailed explanation, see this guide:  
> [Distributed Hadoop Spark Cluster](https://medium.com/@ahmetfurkandemir/distributed-hadoop-cluster-1-spark-with-all-dependincies-03c8ec616166)

## Run
```bash
make
  make wordcount  - Inject a job in containers
  make down       - Stop container
make wordcount