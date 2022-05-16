# 🏃Run pipelines in production using Kubeflow!

## 🖥 Run it locally (Step-by-step)

### Prerequisites
In order to run this example, we need a few tools to allow CoalescenceML to manage a local Kubeflow set up:

* [K3D](https://k3d.io/v5.4.1/#installation) to run a local Kubernetes cluster
* [Kubectl](https://kubernetes.io/docs/tasks/tools#kubectl) to manage Kubernetes and deploy Kubeflow
* [Docker](https://docs.docker.com/get-docker/) to build docker images
* Allow for Docker to use more than 2GB of Ram (relevant to Mac / Windows users of Docker Desktop)

We dditionally need to ensure we have CoalescenceML along with the appropriate integrations.

```bash
# Install python dependencies
$ pip install coalescenceml

# Install CoalescenceML integrations
$ coml integration install kubeflow sklearn mlflow

# Initialize CoalescenceML
$ coml init
```

### Create a local Kubeflow stack
Now that we've finished all the setup and installs, we can set up our CoalescenceML stack. For the local example, we create a stack with 5 parts:
* A **local artifact store** which stores step outputs on your machines hard drive
* A **local metadata store** which stores metadata like pipeline name and step status
* A **local container registry** which hosts the docker images to run our pipeline
* A **Kubeflow orchestrator** which will run our pipeline on Kubeflow
* An **MLFlow experiment tracker** which will track our metrics

You can either use the CLI
```bash
# Make sure to use 5000 for it to work; 5001 didn't and I don't know why
coml container-registry register local_registry --type=default --uri=localhost:5000
coml orchestrator register kubeflow_orchestrator --type=kubeflow --use_k3d=true
coml experiment-tracker register mlflow_tracker --type=mlflow --use_local_backend=true
coml stack register local_kubeflow_stack \
    --artifact-store=default \
    --metadata-store=default \
    --container-registry=local_registry \
    --orchestrator=kubeflow_orchestrator \
    --experiment-tracker=mlflow_tracker
coml stack set local_kubeflow_stack
```

Or use our YAML file that should load a stack for you
```bash
$ coml stack import <>.yaml
$ coml stack set <>
```

### Start up kubeflow locally
CoalescenceML can take care of setting up and configuring a local Kubeflow deployment. All you need to do is run:
```bash
$ coml stack up
```

When the setup is finished (~5-15 minutes), you should see a local URL which you can access in the browser and view the Kubeflow UI.

### Run the pipeline
We can run the pipeline just like before by running the python script!

```bash
$ python run.py
```

This will build docker images, push to the local container registry, and schedule a pipeline run in Kubeflow. Once the script is finished, you can see the pipeline run in the Kubeflow UI.
### Clean up
Once we are done experimenting, we can stop the  Kubernetes cluster and all associated resources by calling:

```bash
coml stack down
```

## ☁️  Run the pipeline in the cloud 

### Prerequisites

### Create Kubeflow cloud stack

### Run the pipeline


