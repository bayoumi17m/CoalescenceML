from deployments.base_deploy_step import BaseDeploymentStep
from config import DeploymentYAMLConfig
import json
import subprocess


class KubernetesDeployer(BaseDeploymentStep):
    """Step class for deploying model to Kubernetes."""

    def __del__(self):
        self.deploy_log.close()

    def run_cmd(self, cmd, start_msg, err_msg):
        print(start_msg)
        proc = subprocess.run(cmd, stdout=subprocess.PIPE,
                              stderr=subprocess.STDOUT, text=True)
        self.deploy_log.write(proc.stdout)
        if not self.suppress_stdout:
            print(proc.stdout)
        if proc.returncode != 0:
            print("Error: {}. See log for more info.".format(err_msg))
            self.deploy_log.write("Failed command: {}\n".format(proc.args))
            exit(1)
        print("done")

    # Change to custom Flask app instead of mlflow build-docker
    def build_model_image(self):
        """Builds a docker image that serves the model.

        The user specifies the model through its uri, and the path to the
        container registry to build the image.
        """
        build_cmd = ["mlflow", "models", "build-docker",
                     "-m", self.model_uri, "-n", self.registry_path]
        self.run_cmd(build_cmd, "Building docker image...",
                     "Failed to build docker image")

    # Not sure if this step is actually needed
    def push_image(self):
        """Pushes the docker image to the provided registry path."""
        self.run_cmd(
            ["docker", "push", self.registry_path],
            "Pushing image to registry...",
            "Failed to push image"
        )

    def config_deployment(self):
        """Configures the deployment.yaml and service.yaml files for deployment."""
        print("Configuring deployment...", end="")
        config = DeploymentYAMLConfig(self.deployment_name, self.registry_path)
        config.create_deployment_yaml()
        config.create_service_yaml()
        print("done")

    def deploy(self):
        """Applies the deployment and service yamls."""
        deploy_cmd = ["kubectl", "apply", "-f", "deployment.yaml"]
        service_cmd = ["kubectl", "apply", "-f", "service.yaml"]
        self.run_cmd(deploy_cmd, "Creating deployment...",
                     "Failed to create deployment")
        self.run_cmd(service_cmd, "Creating LoadBalancer service...",
                     "Failed to create LoadBalancer service")

    def get_service_url(self):
        """Get service URL.

        Searches kubectl for the service name and returns its external IP,
        if any.
        """
        p = subprocess.run(["kubectl", "get", "service",
                           self.service_name, "--output=json"], capture_output=True)
        service_json = json.loads(p.stdout)
        ip = service_json["status"]["loadBalancer"]["ingress"][0]["ip"]
        if ip is None or ip == "":
            print("Error: could not find IP")
            return None
        elif ip == "<pending>":
            print("Ingress IP is currently pending, please try again later.")
        return "http://{}".format(ip)

    def entrypoint(self, model_uri, registry_path, deploy, suppress_stdout=True):
        if not deploy:
            return None
        self.model_uri = model_uri
        self.registry_path = registry_path
        self.deployment_name = "mlflow-deployment"
        self.service_name = self.deployment_name + "-service"
        self.suppress_stdout = suppress_stdout
        self.deploy_log = open("kubedeploy.log", "w")
        self.build_model_image()
        self.push_image()
        self.config_deployment()
        self.deploy()
        url = self.get_service_url()
        return url


kd = KubernetesDeployer()

project_id = "mlflow-gcp-testing"
repo = "mlflow-repo"

url = kd.entry_point(
    "s3://coml-mlflow-models/sklearn-regression-model",
    "k8s.gcr.io/sklearnmodel",
    deploy=True,
    suppress_stdout=False)

print("URL: ", url)
