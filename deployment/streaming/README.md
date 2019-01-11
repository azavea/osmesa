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

Our ECS deployment process relies on the use of the `ecs-cli` tool, which is
similar in spirit to `docker-compose`, but manages containers on ECS instead
of on a local docker instance.  You can install `ecs-cli` by issuing the
command
```bash
 curl -o /usr/local/bin/ecs-cli https://s3.amazonaws.com/amazon-ecs-cli/ecs-cli-linux-amd64-latest
```

## Deployment Steps

1. Copy `config-aws.mk.example` to `config-aws.mk` and
   `config-local.mk.example` to `config-local.mk`. These can be configured in a
   moment.

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

9. Deploy the service (this will create new task definitions as necessary):

```bash
make start-service
```

### Updating an Existing ECS Service

If there is already a streaming task running on an ECS cluster that needs to
be updated, then the procedure above can be abbreviated.  Please perform steps
1, 2, 4, 7, and 9.

## Local Testing

From a clean environment,

1. In the `deployment/streaming` directory, update the missing values in
   `config-local.mk.example` (LOCAL_AUGDIFF_SOURCE, LOCAL_AUGDIFF_START,
   LOCAL_CHANGE_START, LOCAL_CHANGESET_START) and save to `config-local.mk`.

2. In the same directory, ensure that `config-aws.mk` exists.  You may `touch`
   the file if it does not.

3. Execute `make build-container` followed by `make start-local`.  You should
   observe a stream of log messages.  Any errors should appear in this window.

4. If you want to verify that the system is operating up to spec, you may
```bash
docker exec -it streaming_db_1 bash
psql -U postgres
```
(The trailing "1" may need to be incremented.  See `docker ps` for the proper
name.)  From there, you may issue a `\d` directive and verify that the DB is
populated with the correct tables.

5. You may now test the operation of the system in the DB interface by issuing
   queries against the available tables and observing the log output.  The
   content of the tables will update as the system runs.
