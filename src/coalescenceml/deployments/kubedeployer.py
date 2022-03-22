from basedeployer import BaseDeploymentStep
import subprocess


class KubernetesDeployer(BaseDeploymentStep):
    
    # Change to custom Flask app instead of mlflow build-docker
    def build_model_image(self):
        """Builds a docker image that serves the model.

        The user specifies the model through its uri, and the path to the
        container registry to build the image.
        """
        build_command = ["mlflow", "models", "build-docker",
                         "-m", self.model_uri, "-n", self.registry_path]
        print("Building docker image...", end="")
        subprocess.run(build_command, stdout=self.docker_log,
                       stderr=self.docker_log)
        print("done")

    # Not sure if this step is actually needed
    def push_image(self):
        """Pushes the docker image to the provided registry path."""
        print("Pushing image to registry...", end="")
        push_command = ["docker", "push", self.registry_path]
        subprocess.run(push_command, stdout=self.docker_log)
        print("done")

    def config_deployment(self):
        """Configures the deployment.yaml and service.yaml files for deployment."""
        print("Configuring deployment...", end="")
        config_command = ["./config.sh",
                          self.deployment_name, self.registry_path]
        subprocess.run(config_command)
        print("done")

    def deploy(self):
        """Deploys the model."""
        print("Deploying...", end="")
        deploy_command = ["kubectl", "apply", "-f", "deployment.yaml"]
        service_command = ["kubectl", "apply", "-f", "service.yaml"]
        subprocess.run(deploy_command)
        subprocess.run(service_command)
        print("done")

    def get_service_url(self):
        """Get service URL.

        Searches kubectl for the service name and returns its external IP,
        if any.
        """
        p1 = subprocess.Popen(
            ["kubectl", "get", "services"], stdout=subprocess.PIPE)
        p2 = subprocess.Popen(["grep", self.service_name],
                              stdin=p1.stdout, stdout=subprocess.PIPE)
        p1.stdout.close()
        p3 = subprocess.Popen(["awk", "{print $4}"],
                              stdin=p2.stdout, stdout=subprocess.PIPE)
        p2.stdout.close()
        ip = p3.communicate()[0].strip().decode("utf-8")
        if ip is None or ip == "":
            print("Error: could not find IP")
            return None
        return "http://{}".format(ip)

    def entry_point(self, model_uri, registry_path, deploy):
        if not deploy:
            return None
        self.model_uri = model_uri
        self.registry_path = registry_path
        self.deployment_name = "mlflow-deployment"
        self.service_name = self.deployment_name + "-service"
        with open("docker.log", "w") as docker_log:
            self.docker_log = docker_log
        self.build_model_image()
        self.push_image()
        self.config_deployment()
        self.deploy()
        return self.get_service_url()


kd = KubernetesDeployer()

project_id = "mlflow-gcp-testing"
repo = "mlflow-repo"

url = kd.entry_point(
    "s3://coml-mlflow-models/sklearn-regression-model",
    "us-east1-docker.pkg.dev/{}/{}/sklearn-model".format(project_id, repo),
    True)

print("URL: ", url)
