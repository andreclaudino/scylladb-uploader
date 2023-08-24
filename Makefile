IMAGE_REPOSITORY=docker.io/andreclaudino/scylladb-uploader
PROJECT_VERSION := $$(cat Cargo.toml | grep version | head -n 1 | awk '{print $$3}' | sed -r 's/^"|"$$//g')
IMAGE_NAME=$(IMAGE_REPOSITORY):$(PROJECT_VERSION)
GIT_REFERENCE := $$(git log -1 --pretty=%h)

run-storage:
	mkdir -p $(PWD)/minio-data
	podman run \
		-p 9000:9000 \
		-p 9001:9001 \
  		-v $(PWD)/minio-data:/data \
			quay.io/minio/minio \
				server /data \
				--console-address ":9001"


docker/flags/create:
	mkdir -p docker/flags
	touch docker/flags/create


docker/flags/build: docker/flags/create
	podman build -t $(IMAGE_REPOSITORY):latest -f docker/Dockerfile . \
		--build-arg GIT_REFERENCE=$(GIT_REFERENCE) \
		--build-arg VERSION=$(PROJECT_VERSION)
	touch docker/flags/build


docker/flags/login: docker/flags/create
	podman login docker.io
	touch docker/flags/login


docker/flags/tag: docker/flags/build
	podman tag $(IMAGE_REPOSITORY):latest $(IMAGE_NAME)
	touch docker/flags/tag


docker/flags/push: docker/flags/login docker/flags/tag docker/flags/build
	podman push $(IMAGE_REPOSITORY):latest
	podman push $(IMAGE_NAME)


clean:
	rm -rf docker/flags