from basedeployer import BaseDeploymentStep
import subprocess


class KubernetesDeployer(BaseDeploymentStep):


    def build_model_image(self, model_uri, registry_path, docker_log):
        build_command = ["mlflow", "models", "build-docker",
                         "-m", model_uri, "-n", registry_path]
        print("Building docker image...", end="")
        subprocess.run(build_command, stdout=docker_log, stderr=docker_log)
        print("done")

    def push_image(self, registry_path, docker_log):
        print("Pushing image to registry...", end="")
        push_command = ["docker", "push", registry_path]
        subprocess.run(push_command, stdout=docker_log)
        print("done")

    def config_deployment(self, registry_path):
        deployment_name = "mlflow-deployment"
        print("Configuring deployment...", end="")
        config_command = ["./config.sh", deployment_name, registry_path]
        subprocess.run(config_command)
        print("done")

    def deploy(self):
        print("Deploying...", end="")
        deploy_command = ["kubectl", "apply", "-f", "deployment.yaml"]
        service_command = ["kubectl", "apply", "-f", "service.yaml"]
        subprocess.run(deploy_command)
        subprocess.run(service_command)
        print("done")

    def entry_point(self, model_uri, registry_path, deploy):
        with open("docker.log", "w") as docker_log:
            self.build_model_image(model_uri, registry_path, docker_log)
            self.push_image(registry_path, docker_log)
        self.config_deployment(registry_path)
        self.deploy()


kd = KubernetesDeployer()

project_id = "mlflow-gcp-testing"
repo = "mlflow-repo"

kd.entry_point(
    "s3://coml-mlflow-models/sklearn-regression-model",
    "us-east1-docker.pkg.dev/{}/{}/sklearn-model".format(project_id, repo),
    True)
