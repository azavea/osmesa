
variable "service_image" {
  type = "string"
  description = "Stats service Docker image"
}

variable "ec2_key" {
  type = "string"
  description = "EC2 key for EMR and ECS machines"
}

variable "desired_stats_instance_count" {
  default = 1
  description = "Number stats instances to provision"
}

# TODO: make this a dynamic lookup
variable "aws_ecs_ami" {
  default = "ami-6bb2d67c"
}

variable "ecs_instance_type" {
  default = "m3.large"
}

variable "ecs_service_role" {
  default = "arn:aws:iam::896538046175:role/ecs_service_role"
}

variable "ecs_instance_profile" {
  default = "arn:aws:iam::896538046175:instance-profile/terraform-wzxkyowirnachcosiqxrriheki"
}

variable "s3bucket" {
  type = "string"
}

variable "s3prefix" {
  type = "string"
}

variable "subnet_id" {
  type = "string"
}

variable "aws_region" {
  type = "string"
}

