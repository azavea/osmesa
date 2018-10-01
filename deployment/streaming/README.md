# Streaming Stats Deployment via AWS ECS

Amazon ECS is a system for deploying containers on top of AWS managed
infrastructure. ECS is the deployment strategy we've provided resources
for and would suggest because failures and even the hardware hiccups
(say, the loss of a machine) will be automatically resolved so that
the stream can get back to work. In conjunction with a checkpointing
mechanism which ensures the stream starts near where it left off, these
streams are highly resilient.

An ECS deployment consists of a few different pieces:

- The ECS cluster: scales EC2 instances up and down as necessary
- Services: describe long-running programs that should maintain availability
- Tasks: one or more containerized processes being run by the cluster
- Containers: docker images uploaded to AWS ECR to be pulled upon each task creation

The long-running stream which keeps statistics up to date by
continuously polling Overpass augmented diffs and OSM changesets is
deployed as an ECS cluster. This cluster has a service that tracks
a lone streaming task and reboots the stream from the latest saved
checkpoint (which lives on the table 'checkpoints' in the database being
updated) to ensure that failures aren't fatal to the long-running
process.

## Deployment Steps

1. Copy `config-aws.mk.tpl` to `config-aws.mk` and `config-local.mk.tpl`.
   These can be configured in a moment.

2. Build the osm_analytics container

```bash
make build-container
```

3. Create an IAM role for EC2 instances. This will become `INSTANCE_ROLE`.
   The "AmazonEC2ContainerServiceforEC2Role" policy should be attached.

4. Edit [config-aws.mk.tpl](./config-aws.mk.tpl) with variables appropriate
   to your AWS account and desired deployment (choose VPCs, Security Groups,
   etc) and save the file as `config-aws.mk` Note that you will need to provide
   an ECR repo URI (which you'll have to set up manually via the AWS console) in
   order to use your container on AWS.

5. Manually create an ECS cluster backed by EC2 instances (not fargate), and
   be sure to record the cluster name in `config-aws.mk`. It should now be
   possible to configure ECS-CLI to deploy services against your cluster:

```bash
make configure-cluster
```

6. Assuming all's well, you're ready to deploy. Update the docker-compose
   which defines your services with the appropriate variables:

```bash
make docker-compose.deploy.yml
```

7. Push your image to ECR:

```bash
make push-image
```

8. Bring the cluster up:

```bash
make cluster-up
```

8. Deploy the service (this will create new task definitions as necessary):

```bash
make start-service
```