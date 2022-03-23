DEPLOYMENT_TEMPLATE_FILE = "templates/deploy_template.yaml"
DEPLOYMENT_OUTPUT_FILE = "deployment.yaml"
SERVICE_TEMPLATE_FILE = "templates/service_template.yaml"
SERVICE_OUTPUT_FILE = "service.yaml"


def replace_template(template_file, out_file, replacements):
    with open(template_file) as f:
        templ = f.read()
    for find, replace in replacements:
        templ = templ.replace(find, replace)
    with open(out_file, "w") as f:
        f.write(templ)


def create_deployment_yaml(deployment_name, image_name):
    replace_template(
        DEPLOYMENT_TEMPLATE_FILE,
        DEPLOYMENT_OUTPUT_FILE,
        [("DEPLOYMENT_NAME", deployment_name), ("IMAGE_NAME", image_name)])


def create_service_yaml(deployment_name):
    service_name = deployment_name + "-service"
    replace_template(
        SERVICE_TEMPLATE_FILE,
        SERVICE_OUTPUT_FILE,
        [("DEPLOYMENT_NAME", deployment_name), ("SERVICE_NAME", service_name)])
