variable "aws_region" {
  type        = "string"
  description = "AWS Region"
  default = "us-east-1"
}

variable "key_name" {
  type        = "string"
  description = "The name of the EC2 secret key (primarily for SSH access)"
}

variable "s3_uri" {
  type        = "string"
  description = "Where EMR logs will be sent"
  default     = "s3n://geotrellis-test/terraform-logs/"
}

variable "s3_hbase_root_uri" {
  type        = "string"
  description = "The hbase root for s3"
}

variable "bootstrap_bucket" {
  description = "The bucket for temporarily hosting bootstrap scripts"
}

variable "bootstrap_prefix" {
  description = "The prefix for temporarily hosting bootstrap scripts"
}

variable "ecs_ami" {
    type        = "string"
    description = "AMI to use for ECS Instances"
    default     = "ami-9eb4b1e5"
}

variable "query_port" {
    type        = "string"
    description = "The port on which to connect to ecs"
    default     = "8000"
}

variable "worker_count" {
    type        = "string"
    description = "The number of worker nodes"
    default     = "1"
}

variable "subnet" {
  type        = "string"
  description = "The subnet in which to launch the EMR cluster"
}

variable "emr_service_role" {
  type        = "string"
  description = "EMR service role"
  default     = "EMR_DefaultRole"
}

variable "emr_instance_profile" {
  type        = "string"
  description = "EMR instance profile"
  default     = "EMR_EC2_DefaultRole"
}

variable "bid_price" {
  type        = "string"
  description = "Bid Price"
  default     = "0.07"
}
