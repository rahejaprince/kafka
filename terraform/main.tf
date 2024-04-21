
terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
    region = var.ec2_region
}

data "template_file" "user_data" {
  template = file("./cloudinit.yml")
  vars = {
    deployment = var.deployment
    public_key = var.public_key
  }
}

data "cloudinit_config" "user_data" {
  gzip          = false
  base64_encode = true

  part {
    content_type = "text/cloud-config"
    content      = data.template_file.user_data.rendered
    filename     = "cloudinit.yml"
  }
}

{% if spot_instance %}
resource "aws_spot_instance_request" "worker" {
  spot_price = var.spot_price
  wait_for_fulfillment = true
  launch_group = "{{ tags[JenkinsBuildUrl] }}"
  spot_type = "one-time"
  valid_until = "{{ spot_instance_valid_time }}"
{% else %}
resource "aws_instance" "worker" {
{% endif %}
  count= var.num_workers
  ami= var.worker_ami
  instance_type = var.instance_type
  key_name = "semaphore-muckrake"
  subnet_id = "subnet-0429253329fde0351"
  vpc_security_group_ids = ["sg-03364f9fef903b17d"]
  associate_public_ip_address = false

  user_data = data.cloudinit_config.user_data.rendered
  tags = {
    Name = format("kafka-worker-%d", count.index)
    ducktape: "true",
    Owner = "ce-kafka",
    role = "ce-kafka",
    JenkinsBuildUrl = self.args.build_url,
    cflt_environment = "devel",
    cflt_partition = "onprem",
    cflt_managed_by = "iac",
    cflt_managed_id = "kafka",
    cflt_service = "kafka",
    test = "rashiEC2"
    
  }
}

{% if spot_instance %}
resource "null_resource" "spot_instance_tag_command" {
  triggers = {
    cluster_instance_ids = "${join(",", aws_spot_instance_request.worker.*.spot_instance_id)}"
  }
  provisioner "local-exec" {
    command = "aws ec2 create-tags --resources ${join(" ", aws_spot_instance_request.worker.*.spot_instance_id)} --tags {{ aws_tags }}"
  }
}


output "worker-public-ips" { value = aws_spot_instance_request.worker.*.public_ip }
output "worker-public-dnss" { value = aws_spot_instance_request.worker.*.public_dns }
output "worker-private-ips" { value = aws_spot_instance_request.worker.*.private_ip }
output "worker-private-dnss" { value = aws_spot_instance_request.worker.*.private_dns }
output "worker-names" { value = aws_spot_instance_request.worker.*.tags.Name }
{% else %}
output "worker-public-ips" { value = aws_instance.worker.*.public_ip }
output "worker-public-dnss" { value = aws_instance.worker.*.public_dns }
output "worker-private-ips" { value = aws_instance.worker.*.private_ip }
output "worker-private-dnss" { value = aws_instance.worker.*.private_dns }
output "worker-names" { value = aws_instance.worker.*.tags.Name }
{% endif %}
