IMG  := quay.io/azavea/osmesa-base
TAG := "latest"

build: dockerfile
	docker build \
		-f Dockerfile \
                -t ${IMG}:${TAG} .

publish: build
	docker push ${IMG}:${TAG}

test: build
	docker run -it --rm ${IMG}:${TAG} java -version

clean:
