import os

TEMPLATE_DIR = "deploy_templates"
DEPLOYMENT_TEMPLATE_FILE = os.path.join(TEMPLATE_DIR, "deploy_template.yaml")
DEPLOYMENT_OUTPUT_FILE = "deployment.yaml"
SERVICE_TEMPLATE_FILE = os.path.join(TEMPLATE_DIR, "service_template.yaml")
SERVICE_OUTPUT_FILE = "service.yaml"


class DeploymentYAMLConfig:
    """
    A class for configuring Kubernetes YAML files.
    """

    def __init__(self, deployment_name, image_name):
        self.deployment_name = deployment_name
        self.image_name = image_name

    def replace_template(self, template_file, out_file, replacements):
        """
        Reads in a template and writes to an output file.

        Args:
            template_file: A string denoting the path of
                the template file to read from
            out_file: A string denoting the path of
                the file to write the replaced template to.
            replacements: A list of tuples [(f1, r1), ..., (fn, rn)], meaning
                that all occurences of fi will be replaced by ri in the
                output file.
        """
        with open(template_file) as f:
            templ = f.read()
        for find, replace in replacements:
            templ = templ.replace(find, replace)
        with open(out_file, "w") as f:
            f.write(templ)

    def create_deployment_yaml(self):
        """Writes the deployment yaml file from the deployment template."""
        self.replace_template(
            DEPLOYMENT_TEMPLATE_FILE,
            DEPLOYMENT_OUTPUT_FILE,
            [("DEPLOYMENT_NAME", self.deployment_name), ("IMAGE_NAME", self.image_name)])

    def create_service_yaml(self):
        """Writes the service yaml file from the service template."""
        service_name = self.deployment_name + "-service"
        self.replace_template(
            SERVICE_TEMPLATE_FILE,
            SERVICE_OUTPUT_FILE,
            [("DEPLOYMENT_NAME", self.deployment_name), ("SERVICE_NAME", service_name)])

    def remove_safe(self, file):
        """Removes a file if it exists."""
        if os.path.exists(file):
            os.remove(file)

    def cleanup(self):
        """Removes the generated yaml files."""
        self.remove_safe(DEPLOYMENT_OUTPUT_FILE)
        self.remove_safe(SERVICE_OUTPUT_FILE)
