variable "aws_region" {
    type        = "string"
    description = "AWS Region"
    default     = "us-east-1"
}

variable "aws_profile" {
    type        = "string"
    description = "AWS Profile"
}

variable "key_name" {
    type        = "string"
    description = "The name of the EC2 secret key (primarily for SSH access)"
}

variable "worker_count" {
    type        = "string"
    description = "The number of worker nodes"
    default     = "1"
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

variable "user" {
  default = "EMR"
}

variable "cluster_name" {
  default = "Testing"
}

variable "master_instance_type" {
  default = "m3.2xlarge"
}

variable "worker_instance_type" {
  default = "m3.xlarge"
}
