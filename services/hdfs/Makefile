IMG  := quay.io/azavea/osmesa-hdfs
TAG  := latest

build: dockerfile
	docker build \
		-f Dockerfile \
                -t ${IMG}:${TAG}	.

publish: build
	docker push ${IMG}:${TAG}

run: build
	docker run -p 50070:50070 -p 8088:8088 --entrypoint /bin/bash ${IMG}:${TAG}


clean:
