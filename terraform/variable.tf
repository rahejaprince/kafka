variable "ec2_region" {
  type    = string
  description = "EC2 region"
  default = "us-west-2"
}

variable "spot_price" {
  type = string
}

variable "num_workers" {
  type        = string
  description = "number of workers"
}

variable "worker_ami" {
  type        = string
  description = "AMI of aws"
}

variable "instance_type" {
  type        = string
  description = "Instance type of aws"
}

variable "deployment" {
  type        = string
  default     = "ubuntu"
  description = "linux distro you are deploying with, valid values are ubuntu and rpm"
}

variable "public_key" {
  type        = string
  description = "muckrake pem file public key"
}

variable "build_url" {
  type = string
  
}