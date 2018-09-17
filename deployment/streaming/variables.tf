variable "aws_region" {
    type        = "string"
    description = "AWS Region"
    default     = "us-east-1"
}

variable "project" {
  type          = "string"
}

variable "environment" {
  type          = "string"
}

variable "aws_profile" {
    type        = "string"
    description = "AWS Profile"
}

variable "aws_key_name" {
    type        = "string"
    description = "The name of the EC2 secret key (primarily for SSH access)"
}

variable "vpc_id" {
    type        = "string"
    description = "The VPC ID"
}

variable "private_subnet_ids" {
  type          = "list"
  description   = "Private subnets to launch clusters within"
}

variable "container_instance_type" {
  type         = "string"
  default      = "m5.large"
}

variable "changeset_source" {
  type         = "string"
}

variable "augdiff_source" {
  type         = "string"
}

variable "database_url" {
  type         = "string"
}

variable "start_sequence" {
  type         = "string"
  default      = 1
}
