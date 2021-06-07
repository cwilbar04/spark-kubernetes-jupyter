# spark-kubernetes-jupyter

## Overview
This repository attempts to provide the code to sucessfully create a Spark environment deployed to a Google Kubernetes Enginer cluster configured with the brand new Autopilot mode and using the recently full released option of using the Kubernetes API as a cluster manager. Additionally, submitting Spark jobs is attempted to be performmed interactively by creating a SparkSession in a Jupyter Lab environment running on a separate container.  

The [Workbook](/Project Creation Workbook.ipynb) walks through creating the Google Cloud environment and deploying the correct containers to attempt creating an intereactive Spark environment.
  
Unfortunately, I was not able to get the Spark executor containers to launch sucessfully in order, however, I was able to create the Kubernetes cluster and create a Spark Session but the Spark clusters continuously fail.

This repository also contains the code to run distirbuted spark using the Spark Standalone cluster manager using the docker compose file in the cluster-mode-standalone filepath.

## Standalone Mode
The [cluster-mode-standalone](/cluster-mode-standalone) folder contains all of the code necessary to create a local environment with a Spark Cluster that is managed by Spark's Standalone Cluster manager. The Standalone Cluster manager requires two separate docker images, found in [spark-master](/spark-master) and [spark-worker](/spark-worker) where the master image works to schedule executors using the worker images. You then have to manually add how many worker nodes you want in to the docker compose file and it will not dynamically create more if you run out of memory/CPU. The spark master host and port are also hard-coded in to the spark-base image so that the appropriate master image is created and the worker images know what master image to communicate with. Currently, you will see the docker compose file creates 2 worker nodes and it would be tedious to build the amount necessary for a true production environment.  

In order to run your own test of the Standalone images simply clone this repository and execute:
```cmd
make cluster_standalone_build
make cluster_standalone_compose
```  
  
In the output logs find where the jupyterlab container output the link to access the lab. Click on it to launch a Jupyter Lab Session. Check out the Data Load Testing workbook for hints on how to create a SparkSession. Note, however, that I was not able to successfully mount a shared volume so downloading the data does not work as expected.

## Kubernetes Mode
Running spark in cluster mode using the Kubernetes API as the cluster manager is supposed to solve the problem of manually defining each worker node. The Kubernetes API takes a single container image when creating a Spark Session and uses the master API for the Kubernetes cluster as the master node. This means you can take advantage of the autoscaling in Kubernetes to scale up and scale down the number of clusters as needed. Ultimately, this means you only need to deploy the spark-base image and use the jupyterlab image to create a SparkSession to connect to a kubernetes cluster you have created and point to the spark-base image for the worker nodes. Unfortunately, I was not able to determine exactly what entrypoint is needed to get the spark base image to actually launch sucessfully once created. I attempted to only slightly modify the code provided by Spark and that is in ths [spark-provided-images](/spark-provided-images) folder but those still did not work. Future work is needed to determine exactly how the driver nodes need to launch to be ready to accept instructions from the Kubernetes API. Perhaps it jsut was not ready for Kubernetes Enginer Autopilot mode and launching a SparkSession from Jupyterlab.

## Getting Started
Walk through the steps in the [workbook](Project Creation Workbook.ipynb) notebook to:   
- Create your own GCP Project
- Enable the necessary APIs  
- Create servie accounts and permission appropriately  
- Create a kubernetes engine cluster with secret and spark service account
- Build and push Docker Container to Google Container Registry  
- Run Jupyterlab Docker container and see the getting-started notebook to re-create the error I ran in to
