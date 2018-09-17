SBT_DIR="../../src"
JAR_DIR=$(SBT_DIR)/analytics/target/scala-2.11/
DOCKER_DIR="./"


clean:
	rm -f build
	rm -f build-containers
	rm -f tfplan
	rm -f $(DOCKER_DIR)/osmesa-analytics.jar
	cd $(SBT_DIR) && sbt "project analytics" clean

build:
	cd $(SBT_DIR) && sbt "project analytics" assembly
	cp $(JAR_DIR)/osmesa-analytics.jar $(DOCKER_DIR)/osmesa-analytics.jar
	touch build

plan:
	rm -f tfplan
	terraform init
	terraform plan -var-file=tfvars -out=tfplan

apply: tfplan
	terraform apply tfplan

refresh:
	terraform refresh -var-file=tfvars

destroy:
	terraform destroy -var-file=tfvars

validate:
	terraform validate -var-file=tfvars

build-containers: build
	docker build -f $(DOCKER_DIR)/Dockerfile --tag osm_analytics:latest $(DOCKER_DIR)
	touch build-containers

rebuild-containers:
	docker build -f $(DOCKER_DIR)/Dockerfile --tag osm_analytics:latest $(DOCKER_DIR)

push-containers: build-containers
	$(shell aws ecr get-login --no-include-email --region us-east-1)
	docker tag osm_analytics:latest $(shell terraform output osm_analytics_container_url)
	docker push $(shell terraform output osm_analytics_container_url):latest

