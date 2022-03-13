DEPLOYMENT_NAME="mlflow-deployment"
IMAGE_NAME="us-east1-docker.pkg.dev/mlflow-gcp-testing/mlflow-repo/sklearn-model"
SERVICE_NAME=${DEPLOYMENT_NAME}-service

# Edit deployment template
cp templates/deploy_template.yaml deployment.yaml
sed -i "" "s|DEPLOYMENT_NAME|$DEPLOYMENT_NAME|" deployment.yaml
sed -i "" "s|IMAGE_NAME|$IMAGE_NAME|" deployment.yaml

# Edit service template
cp templates/service_template.yaml service.yaml
sed -i "" "s|SERVICE_NAME|$SERVICE_NAME|" service.yaml
sed -i "" "s|DEPLOYMENT_NAME|$DEPLOYMENT_NAME|" service.yaml
