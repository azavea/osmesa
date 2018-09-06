# OSMESA EMR

This directory contains a make file to spin up an EMR cluster using [terraform](https://github.com/hashicorp/terraform).

- [Requirements](#requirements)
- [Makefile](#makefile)
- [Running](#running)

## Requirements

[Terraform 0.11.5](https://github.com/hashicorp/terraform/releases/tag/v0.11.5)

## Settings

[cluster/variables.tf](cluster/variables.tf) contains the full set of variables
which can be specified to modify an EMR deployment. Only those not
provided defaults need to be specified, and these can be found within
[tfvars.tpl](tfvars.tpl) - be sure to make a copy of this template and remove
'tpl' from the filename.


## Makefile

| Command               | Description
|-----------------------|------------------------------------------------------------|
|auth.json              |Generate temporary session and key/secret                   |
|validate-cluster       |`terraform validate - Validate terraform                    |
|init-cluster           |`terraform init` - Initialize terraform                     |
|cluster-tfplan         |`terraform plan` - Plan out an 'apply'  of this terraform   |
|cluster                |`terraform` init, if it's the first run                     |
|ssh                    |SSH into a running EMR cluster                              |
|destroy-cluster        |Destroy a running EMR cluster                               |
|print-vars             |Print out env vars for diagnostic and debug purposes        |

## Running

The Makefile in this directory provides commands to easily set up an EMR
cluster with MFA, but doing so does require a minimal amount of configuration.
It will be necessary to export your desired AWS profile as well as
having set up `assume role` permissions and an MFA device for the AWS
profile exported. You'll also need to make a copy of tfvars.tpl for
adding parameters specific to your deployment.

```bash
export AWS_PROFILE=my_profile
cp tfvars.tpl tfvars
# update tfvars with values appropriate to the EMR cluster you'd like
make auth.json
make cluster
```

`make auth.json` will prompt you for your MFA key and produce a
session which terraform can use to get around MFA restrictions.

**Note:** long startup times (10 minutes or more) probably indicates that you have
chosen a spot price that is too low.

This basic cluster will have a running Zeppelin interface that can be accessed
via SSH tunnel with
[foxyproxy](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-connect-master-node-proxy.html)

![Zeppelin Welcome](./images/zeppelin-welcome.png)

This cluster will not have access to any code until we upload the
appropriate jars and register them within zeppelin

```bash
make upload-assembly
```

is issued.  Upon doing so, you must configure Zeppelin to recognize this
resource by going to the interpreters tab:

![Zeppelin interpreters](./images/zeppelin-interpreters.png)

Edit the spark interpreter settings by adding the GeoTrellis jar into the
class path (`make upload-assembly` copies the fat jar into, e.g.,
`/tmp/geotrellis-spark-etl-assembly-1.2.0-SNAPSHOT.jar`):

![Zeppelin interpreter edit](./images/zeppelin-interpreter-edit.png)

You may then create a new notebook:

![Zeppelin Osmesa Notebook](./images/zeppelin-osmesa-notebook.png)

wherein GeoTrellis deps can be imported:

![Zeppelin Osmesa example](./images/zeppelin-osmesa-example.png)
