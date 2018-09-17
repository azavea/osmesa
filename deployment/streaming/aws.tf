

# Marks AWS as a resource provider.
provider "aws" {
  profile = "${var.aws_profile}"
  region  = "${var.aws_region}"
}

