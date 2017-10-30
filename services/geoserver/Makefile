BASE := $(subst -, ,$(notdir ${CURDIR}))
ORG  := geomesa
REPO := $(word 2, ${BASE})
IMG  := quay.io/${ORG}/${REPO}
GEOMESA_DIST_TARBALL := geomesa-accumulo-dist_2.11-${GEOMESA_VERSION}-bin.tar.gz
GEOMESA_ACCUMULO_GS_INSTALL := geomesa-accumulo-gs-plugin_2.11-${GEOMESA_VERSION}-install.tar.gz

build: ${GEOMESA_ACCUMULO_GS_INSTALL} dockerfile
	docker build \
                --build-arg GEOMESA_VERSION=${GEOMESA_VERSION} \
                --build-arg TAG=${TAG} \
                --build-arg ACCUMULO_VERSION=${ACCUMULO_VERSION} \
                --build-arg THRIFT_VERSION=${THRIFT_VERSION} \
		-f target/Dockerfile \
                -t ${IMG}:${TAG}	.

dockerfile:
	mkdir -p target
	sed 's/__TAG__/'"${TAG}"'/' Dockerfile.template > target/Dockerfile


.cache/${GEOMESA_DIST_TARBALL}:
	mkdir -p .cache \
        && cp ${HOME}/.m2/repository/org/locationtech/geomesa/geomesa-accumulo-dist_2.11/${GEOMESA_VERSION}/${GEOMESA_DIST_TARBALL} .cache

${GEOMESA_ACCUMULO_GS_INSTALL}: .cache/${GEOMESA_DIST_TARBALL}
	tar -zx --strip-components=3 \
		-f .cache/${GEOMESA_DIST_TARBALL} \
    geomesa-accumulo_2.11-${GEOMESA_VERSION}/dist/gs-plugins/${GEOMESA_ACCUMULO_GS_INSTALL}
	@touch -am ${GEOMESA_ACCUMULO_GS_INSTALL}

publish: build
	docker push ${IMG}:${TAG}

clean:
	rm -rf .cache
	rm -rf ${GEOMESA_ACCUMULO_GS_INSTALL}
