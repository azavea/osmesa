IMG  := quay.io/azavea/osmesa-client
TAG  := latest

assembly: proto
	sbt -J-Xms2048m -J-Xmx2048m "assembly"
	touch assembly

build: assembly
	docker build \
    -f Dockerfile \
    -t ${IMG}:${TAG}  .
	touch build

publish: build
	docker push ${IMG}:${TAG}
	touch publish

run: build
	docker run -p 50070:50070 -p 8088:8088 --entrypoint /bin/bash ${IMG}:${TAG}
	touch run

clean:
	rm -rf proto assembly build publish run
	rm -rf protobuf/compiled/*
	rm -rf client/target/scala-2.11/osmesa-client.jar
	rm -rf analytics/target/scala-2.11/osmesa-analytics.jar
	rm -rf ingest/target/scala-2.11/osmesa-ingest.jar
	rm -rf build publish run

