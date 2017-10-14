IMG  := quay.io/azavea/osmesa-hbase
TAG  := "latest"

build: dockerfile
	docker build \
		-f Dockerfile \
                -t ${IMG}:${TAG} .

publish: build
	docker push ${IMG}:${TAG}

run: build
	docker run \
		-p 2181:2181 \
		-p 60010:60010 \
		-p 60000:60000 \
		-p 60020:60020 \
		-p 60030:60030 \
		-h hbase \
		${IMG}:${TAG}

clean:

# docker run -it -p 2181:2181 -p 60010:60010 -p 60000:60000 -p 60020:60020 -p 60030:60030 -h hbase quay.io/azavea/osmesa-hbase:latest /bin/bash
