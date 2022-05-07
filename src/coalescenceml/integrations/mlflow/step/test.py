from coalescenceml.integrations.mlflow.step.base_mlflow_deployer import BaseDeployerConfig
from coalescenceml.integrations.mlflow.step.kubedeployer import KubernetesDeployer
from coalescenceml.integrations.mlflow.step.localdeployer import LocalDeployer
from coalescenceml.logger import get_logger

logger = get_logger(__name__)

config = BaseDeployerConfig()
config.image_name = "sk-learn-wine"
# deployer = LocalDeployer()

# deployer.entrypoint(
#     #model uri
#     "file://C:/Users/anime/OneDrive/Desktop/Stash/mlruns/0/994a67420bf24e5789bfb92677bfbe16/artifacts/potato",
#     #config
#     config,
#     port=5002
# )
model_uri = "file://C:/Users/anime/OneDrive/Desktop/Stash/mlruns/0/994a67420bf24e5789bfb92677bfbe16/artifacts/potato"
image_path = "us-east1-docker.pkg.dev/coalescenceml/hello-repo/sk-learn-wine"
deployment_name = "mlflow-deployment"
service_name = "mlflow-deployment-service"
# us-east1-docker.pkg.dev
deployer = KubernetesDeployer()
# deployer.entrypoint(model_uri,
#     config
# )

deployer.build_model_image(model_uri, image_path)
deployer.push_image(image_path)
yaml_config = deployer.config_deployment(deployment_name, image_path)
deployer.deploy()
yaml_config.cleanup()
deployment_info = deployer.get_deployment_info(service_name)
#not sure how else to display deployment info
logger.info(deployment_info)
