# docker compose run
# scale : num replicate images
# build : build new images if 배포, dont do this
docker compose up --force-recreate --scale worker=4 --scale tester=1 --build

# stop and remove all containers
docker compose down

# docker image to tar
docker save -o docker_images/paddle_tester.tar 2d3c521c3402
docker save -o docker_images/paddle_worker.tar dd4d4f093e5e
docker save -o docker_images/redis.tar 13105d2858de

# tar to docker image
docker load -i docker_images/paddle_worker.tar

