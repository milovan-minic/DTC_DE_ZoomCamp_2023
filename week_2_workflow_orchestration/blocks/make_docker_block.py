from prefect.infrastructure.docker import DockerContainer

# alternative to creating DockerContainer block in the UI
docker_block = DockerContainer(
    image = 'milovanminic/prefect:dtc-de',
    image_pull_policy = 'ALWAYS',
    auto_remove = True
)

docker_block.save('dtc-de', overwrite = True)
