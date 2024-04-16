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

# {% if spot_instance %}
# resource "aws_spot_instance_request" "worker" {
#   spot_price           = var.spot_price
#   wait_for_fulfillment = true
#   //launch_group = "{{ tags[JenkinsBuildUrl] }}"
#   spot_type = "one-time"
#   valid_until = "{{ spot_instance_valid_time }}"
# }
# {% else %}
# resource "aws_instance" "worker" {

#   count= var.num_workers
#   ami= var.worker_ami
#   instance_type = var.instance_type
#   key_name = "semaphore-muckrake"
#   subnet_id = "subnet-0429253329fde0351"
#   vpc_security_group_ids = ["sg-03364f9fef903b17d"]
#   associate_public_ip_address = false

#   user_data = data.cloudinit_config.user_data.rendered
#   tags = {
#     Name = format("muckrake-worker-%d", count.index)
#   }
# }
# {% endif %}

{% if spot_instance %}
resource "aws_spot_instance_request" "worker" {
  spot_price           = var.spot_price
  wait_for_fulfillment = true
  //launch_group         = "{{ tags[JenkinsBuildUrl] }}"
  spot_type            = "one-time"
  valid_until          = "{{ spot_instance_valid_time }}"
  
  count= var.num_workers
  ami= var.worker_ami
  instance_type = var.instance_type
  key_name = "semaphore-muckrake"
  subnet_id = "subnet-0429253329fde0351"
  vpc_security_group_ids = ["sg-03364f9fef903b17d"]
  associate_public_ip_address = false
  
  user_data = data.cloudinit_config.user_data.rendered
  tags = {
    Name = format("muckrake-worker-%d", count.index)
  }
}
{% else %}
resource "aws_instance" "worker" {
  count= var.num_workers
  ami= var.worker_ami
  instance_type = var.instance_type
  key_name = "semaphore-muckrake"
  subnet_id = "subnet-0429253329fde0351"
  vpc_security_group_ids = ["sg-03364f9fef903b17d"]
  associate_public_ip_address = false
  
  user_data = data.cloudinit_config.user_data.rendered
  tags = {
    Name = format("muckrake-worker-%d", count.index)
  }
}
{% endif %}


{% if spot_instance %}
resource "null_resource" "spot_instance_tag_command" {
  triggers = {
    cluster_instance_ids = "${join(",", aws_spot_instance_request.worker.*.spot_instance_id)}"
  }
  provisioner "local-exec" {
    command = "aws ec2 create-tags --resources ${join(" ", aws_spot_instance_request.worker.*.spot_instance_id)} --tags {{ aws_tags }}"
  }
}

# output "worker-public-ips" { value = aws_spot_instance_request.worker.*.public_ip }
# output "worker-public-dnss" { value = aws_spot_instance_request.worker.*.public_dns }
# output "worker-private-ips" { value = aws_spot_instance_request.worker.*.private_ip }
# output "worker-private-dnss" { value = aws_spot_instance_request.worker.*.private_dns }
# output "worker-names" { value = aws_spot_instance_request.worker.*.tags.Name }

# output "worker-public-ips" { value = aws_instance.worker.*.public_ip }
# output "worker-public-dnss" { value = aws_instance.worker.*.public_dns }
# output "worker-private-ips" { value = aws_instance.worker.*.private_ip }
# output "worker-private-dnss" { value = aws_instance.worker.*.private_dns }
# output "worker-names" { value = aws_instance.worker.*.tags.Name }


/*
# Terraform configuration for VirtualBox VM
provider "virtualbox" {
  # Provider-specific configurations can be defined here

}

resource "null_resource" "virtualbox_configuration" {
  # This resource doesn't actually create anything, but acts as a placeholder
  # for running local-exec provisioners.

  # Trigger the execution whenever any of the following values change
  triggers = {
    base_box      = var.base_box
    ram_megabytes = var.ram_megabytes
  }

  provisioner "local-exec" {
    command = <<-EOT
      VBoxManage modifyvm <virtualbox> --memory ${var.ram_megabytes}
    EOT
  }

  # Note: There isn't a direct equivalent to vagrant-cachier in Terraform
  # You may need to handle caching separately outside of Terraform
}

resource "virtualbox_vm" "example_vm" {
  # other VM configurations...

  # Set the cache scope if the vagrant-cachier plugin is installed
  override = var.has_vagrant_cachier_plugin ? {
    cache = "box"
  } : {}
} 


# Override the instance type if the INSTANCE_TYPE environment variable is set
locals {
  overridden_instance_type = coalesce(var.INSTANCE_TYPE, var.ec2_instance_type) //coalesce will return first non-null value from left to right 
}

# Choose size based on the overridden instance type
locals {
  ebs_volume_size = regex("^m3.*", local.overridden_instance_type) ? 20 : 40  #regrex will check the regular expression as "m3" starts with
}
*/