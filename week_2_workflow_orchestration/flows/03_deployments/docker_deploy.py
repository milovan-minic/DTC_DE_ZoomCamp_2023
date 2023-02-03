from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from parameterized_flow import etl_parent_flow

docker_container_block = DockerContainer.load('dtc-de-docker')

docker_deployment = Deployment.builf_from_flow(
    flow = etl_parent_flow,
    name = 'docker_flow',
    infrastructure = docker_container_block
)
