IMG  := quay.io/azavea/osmesa-zookeeper
TAG := "latest"

build: dockerfile
	docker build \
		-f Dockerfile \
                -t ${IMG}:${TAG}	.

publish: build
	docker push ${IMG}:${TAG}

test: build
	docker run -it --rm ${IMG}:${TAG} bash -c "zkServer.sh start; zkCli.sh stat /zookeeper"
