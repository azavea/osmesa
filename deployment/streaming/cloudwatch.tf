#
# Logging
#
resource "aws_cloudwatch_log_group" "log_group" {
  name = "${var.environment}Stream"
}
