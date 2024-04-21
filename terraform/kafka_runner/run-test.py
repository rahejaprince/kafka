import argparse
import json
import logging
import os
import shutil
import sys
import requests
import subprocess


import time
from datetime import datetime, timedelta, timezone
from functools import partial
from traceback import format_exc
from jinja2 import Environment, FileSystemLoader
from ducktape.utils.util import wait_until
from paramiko.ssh_exception import NoValidConnectionsError

from terraform.kafka_runner.util import run, SOURCE_INSTALL,ssh
from terraform.kafka_runner.util import INSTANCE_TYPE,ABS_KAFKA_DIR
from terraform.kafka_runner.util import AWS_REGION, AWS_ACCOUNT_ID, AMI
from terraform.kafka_runner.package_ami import package_worker_ami


def tags_to_aws_format(tags):
    kv_format = [f"Key={k},Value={v}" for k,v in tags.items()]
    return f"{' '.join(kv_format)}"


def setup_virtualenv(venv_dir, args):
    """Install virtualenv if necessary, and create VIRTUAL_ENV directory with virtualenv command."""
    if run("which virtualenv") != 0:
        # install virtualenv if necessary
        logging.info("No virtualenv found. Installing...")
        run(f"{args.python} -m pip install virtualenv",
            allow_fail=False)
        logging.info("Installation of virtualenv succeeded")

    if not os.path.exists(venv_dir):
        logging.info("Setting up virtualenv...")
        run(f"virtualenv -p {args.python} {venv_dir}", allow_fail=False)
        run("pip install --upgrade pip setuptools", venv=True, allow_fail=False)

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--aws", action="store_true", help="use commands for running on AWS")
    parser.add_argument("--image-name", action="store", type=str, default=os.environ.get('IMAGE_NAME'),
                        help="Name of image to use for virtual machines.")
    parser.add_argument("--install-type", action="store", default=SOURCE_INSTALL,
                        help="how Confluent Platform will be installed")
    parser.add_argument("--instance-name", action="store", default="KAFKA_TEST_JENKINS",
                        help="Name of AWS instances")
    parser.add_argument("--worker-instance-type", action="store", default=INSTANCE_TYPE,
                        help="AWS instance type to be used for worker nodes.")
    parser.add_argument("--worker-volume-size", action="store", type=int,
                        default=40, help="Volume size in GB to be used for worker nodes. Min 50GB.")
    parser.add_argument("--vagrantfile", action="store", default="system-test/resources/scripts/system-tests/kafka-system-test/Vagrantfile.local",
                        help="specify location of template for Vagrantfile.local")
    parser.add_argument("--num-workers", action="store", type=int,
                        default=10, help="number of worker nodes to bring up")
    parser.add_argument("--enable-pwd-scan", action="store_true",
                        help="run password scanner with --enable-pwd-scan flag."
                            "Scan for passwords from constants file in logs ")
    parser.add_argument("--notify-slack-with-passwords", action="store_true",
                        help="Notify slack channel with passwords found")
    parser.add_argument("--results-root", action="store", default="./results", help="direct test output here")
    parser.add_argument('test_path', metavar='test_path', type=str, nargs='*',
                        default=["tests/kafkatest/tests/core/security_test.py"],
                        help="run tests found underneath this directory. "
                             "This directory is relative to root muckrake directory.")
    parser.add_argument("--collect-only", action="store_true", help="run ducktape with --collect-only flag")
    parser.add_argument("--cleanup", action="store", type=parse_bool, default=True,
                        help="Tear down instances after tests.")
    parser.add_argument("--repeat", action="store", type=int, default=1,
                        help="Use this flag to repeat all discovered tests the given number of times.")
    parser.add_argument("--resource-url", action="store",
                        help="if using tarball or distro install type, specify URL path to CP tarball or CP distro [deb or rpm]")
    parser.add_argument("--parallel", action="store_true", help="if true, run tests in parallel")
    parser.add_argument("--linux-distro", action="store", type=str, default="ubuntu",
                        help="The linux distro to install on.")
    parser.add_argument("--sample", action="store", type=int, default=None,
                        help="The size of a random sample of tests to run")
    parser.add_argument("--python", action="store", type=str, default="python", help="The python executable to use")
    parser.add_argument("--build-url", action="store", type=str, default="qe.test.us",
                        help="The Jenkins Build URL to tag AWS Resources")
    parser.add_argument("--parameters", action="store", type=str, default=None, help="Override test parameter")
    parser.add_argument("--spot-instance", action="store_true", help="run as spot instances")
    parser.add_argument("--spot-price", action="store", type=float, default=0.266, help="maximum price for a spot instance")
    """
    parser.add_argument("--ssh-checker", action="store", nargs='+',
                        default=['muckrake.ssh_checkers.aws_checker.aws_ssh_checker'],
                        help="full module path of functions to run in the case of an ssh error")
    """
    parser.add_argument("--jdk-version", action="store", type=str, default="8",
                        help="JDK version to install on the nodes."),
    parser.add_argument("--nightly", action="store_true", default=False, help="Mark this as a nightly run")
    parser.add_argument("--new-globals", action="store", type=str, default=None, help="Additional global params to be passed in ducktape")
    parser.add_argument("--arm-image", action="store_true", help="load the ARM based image of specified distro")
    args, rest = parser.parse_known_args(sys.argv[1:])

    validate_args(args)
    return args, rest

def validate_args(args):
    if args.install_type != SOURCE_INSTALL:
        assert args.resource_url, "If running a non-source install, a resource url is required."
    resource_response = check_resource_url(args.resource_url)
    assert resource_response, "resource url is either invalid or the connection has failed"
    
def check_resource_url(resource_url):
    if resource_url.endswith('.tar.gz'):
        return requests.get(resource_url).ok
    if '/rpm/' in resource_url or '/deb/' in resource_url:
        return requests.get(resource_url + '/archive.key').ok
    return True

def parse_bool(s):
    return True if s and s.lower() not in ('0', 'f', 'no', 'n', 'false') else False  



def image_from(name=None, image_id=None, region_name=AWS_REGION):
    """Given the image name or id, return a boto3 object corresponding to the image, or None if no such image exists."""
    if bool(name) == bool(image_id):
        raise ValueError('image_from requires either name or image_id')

    ec2 = boto3.resource("ec2", region_name=region_name)

    filters = []
    if image_id:
        filters.append({'Name': 'image-id', 'Values': [image_id]})
    if name:
        filters.append({'Name': 'name', 'Values': [name]})

    return next(iter(ec2.images.filter(Owners=[AWS_ACCOUNT_ID], Filters=filters)), None)



class kafka_runner:
    kafka_dir=ABS_KAFKA_DIR
    cluster_file_name = f"{kafka_dir}/tf-cluster.json"
    tf_variables_file = f"{kafka_dir}/tf-vars.tfvars.json"

    def __init__(self, args, venv_dir):
        self.args = args
        self._terraform_outputs = None
        self.venv_dir = venv_dir
        self.public_key = os.environ['MUCKRAKE_SECRET']

    
    def _run_creds(self, cmd, *args, **kwargs):
        return run(f". assume-iam-role arn:aws:iam::419470726136:role/semaphore-access> /dev/null; cd {self.kafka_dir}; {cmd}", *args, **kwargs)


    def terraform_outputs(self):
        if not self._terraform_outputs:
            raw_json = self._run_creds(f"terraform output -json", print_output=True, allow_fail=False,
                           return_stdout=True, cwd=self.kafka_dir)
            self._terraform_outputs = json.loads(raw_json)
        return self._terraform_outputs
     
    def update_hosts(self):
        cmd = "sudo bash -c 'echo \""
        terraform_outputs_dict = self.terraform_outputs()
        worker_names = terraform_outputs_dict['worker-names']["value"]
        worker_ips = terraform_outputs_dict['worker-private-ips']["value"]

        for hostname, ip in zip(worker_names, worker_ips):
            cmd += f"{ip} {hostname} \n"
        cmd += "\" >> /etc/hosts'"
        run_cmd = partial(ssh, command=cmd)

        for host in worker_ips:
            run_cmd(host)
        run(cmd, print_output=True, allow_fail=False)

    def generate_clusterfile(self):
        print("generating cluster file")
        terraform_outputs_dict = self.terraform_outputs()
        worker_names = terraform_outputs_dict['worker-names']["value"]
        worker_ips = terraform_outputs_dict['worker-private-ips']["value"]

        nodes = []

        for hostname, ip in zip(worker_names, worker_ips):
            nodes.append({
                "externally_routable_ip": ip,
                "ssh_config": {
                    "host": hostname,
                    "hostname": hostname,
                    "port": 22,
                    "user": "terraform",
                    "password": None,
                    "identityfile": "./semaphore-muckrake.pem"
                }
            })
        with open(self.cluster_file_name, 'w') as f:
            json.dump({"nodes": nodes}, f)

    def wait_until_ready(self, timeout=120, polltime=2):
        terraform_outputs_dict = self.terraform_outputs()
        worker_ips = terraform_outputs_dict['worker-private-ips']["value"]

        start = time.time()

        print("path of WORKSPACE DIRECTORY")
        
        # Get the path of the $WORKSPACE directory
        workspace_path = os.environ.get('WORKSPACE')
        print(workspace_path)

        # Check if the $WORKSPACE environment variable is set
        if workspace_path:
            # List all files and directories in the $WORKSPACE directory
            files_and_dirs = os.listdir(workspace_path)

            # Print the list of files
            print("Files in $WORKSPACE directory:")
            for item in files_and_dirs:
                if os.path.isfile(os.path.join(workspace_path, item)):
                    print(item)
        else:
            print("$WORKSPACE environment variable is not set.")

        
        def check_node_boot_finished(host):
            # command to check and see if cloud init finished
            code, _, _ = ssh(host, "[ -f /var/lib/cloud/instance/boot-finished ]")
            return 0 == code
        
        def check_for_ssh(host):
            try:
                ssh(host, "true")
                return True
            except NoValidConnectionsError:
                return False


        def poll_all_nodes():
            # check and see if cloud init is done on all nodes
            unfinished_nodes = [ip for ip in worker_ips if not check_node_boot_finished(ip)]
            result = len(unfinished_nodes) == 0
            if not result:
                time_diff = time.time() - start
                logging.warning(f"{time_diff}: still waiting for {unfinished_nodes}")

            return result

        wait_until(lambda: all(check_for_ssh(ip) for ip in worker_ips),
                    timeout, polltime, err_msg="ssh didn't become available")

        self.update_hosts()
        logging.warning("updated hosts file")

        wait_until(poll_all_nodes, 15 * 60, 2, err_msg="didn't finish cloudinit")
        logging.info("cloudinit finished on all nodes")


    def tags_to_aws_format(tags):
        kv_format = [f"Key={k},Value={v}" for k,v in tags.items()]
        return f"{' '.join(kv_format)}"

    def generate_tf_file(self):
        
        print("creating terraform file")
        env = Environment(loader=FileSystemLoader(f'{self.kafka_dir}'))
        template = env.get_template('main.tf')

        # this spot instance expiration time.  This is a failsafe, as terraform
        # should cancel the request on a terraform destroy, which occurs on a provission
        # failure
        spot_instance_time = datetime.now(timezone.utc) + timedelta(hours=2)
        spot_instance_time = spot_instance_time.isoformat()
        tags = {
            "Name": "kafka-worker",
            "ducktape": "true",
            "Owner": "ce-kafka",
            "role": "ce-kafka",
            "JenkinsBuildUrl": self.args.build_url,
            "cflt_environment": "devel",
            "cflt_partition": "onprem",
            "cflt_managed_by": "iac",
            "cflt_managed_id": "kafka",
            "cflt_service": "kafka",
            "test": "rashiEC2"
        }
        with open(f'{self.kafka_dir}/main.tf', 'w') as f:
            f.write(template.render(spot_instance=self.args.spot_instance,
                                    spot_instance_valid_time=spot_instance_time,
                                    tags=tags,
                                    aws_tags=tags_to_aws_format(tags)))
        print("terraform file created")
               
    def setup_tf_variables(self, ami):
        vars = {
            "instance_type": self.args.worker_instance_type,
            "worker_ami": ami,
            "num_workers": self.args.num_workers,
            "deployment": self.args.linux_distro,
            "public_key": self.public_key,
            "spot_price": self.args.spot_price
        }

        with open(self.tf_variables_file, 'w') as f:
            json.dump(vars, f)

    def provission_terraform(self):
        print("provisioning tf file")
        self._run_creds(f"terraform --version", print_output=True, allow_fail=False)
        self._run_creds(f"terraform init", print_output=True, allow_fail=False, venv=False, cwd=self.kafka_dir)
        self._run_creds(f"terraform apply -auto-approve -var-file={self.tf_variables_file}", print_output=True, allow_fail=False,
            venv=False, cwd=self.kafka_dir)

    def destroy_terraform(self, allow_fail=False):
        self._run_creds(f"terraform init", print_output=True, allow_fail=True, cwd=self.kafka_dir)

        self._run_creds(f"terraform destroy -auto-approve -var-file={self.tf_variables_file}", print_output=True,
            allow_fail=allow_fail, cwd=self.kafka_dir)
        
    
    
def main():
    logging.basicConfig(format='[%(levelname)s:%(asctime)s]: %(message)s', level=logging.INFO)
    args, ducktape_args = parse_args()
    kafka_dir = ABS_KAFKA_DIR
    build_url= args.build_url


    if args.new_globals is not None:
        global_val = json.loads(args.new_globals)
        file_data = open(f'{kafka_dir}/resources/{args.install_type}-globals.json', 'r')
        globals_dict = json.loads(file_data.read())
        file_data.close()
        for key, value in global_val.items():
            globals_dict[key] = value

        with open(f'{kafka_dir}/resources/{args.install_type}-globals.json', "w") as outfile:
            json.dump(globals_dict, outfile, indent = 4)

        print(f"New globals passed - {globals_dict}")

    kafka_dir = ABS_KAFKA_DIR
    venv_dir = os.path.join(kafka_dir, "venv")

# setup virtualenv directory
    if os.path.exists(venv_dir):
        shutil.rmtree(venv_dir, ignore_errors=True)
        setup_virtualenv(venv_dir, args)

# reset directory containing source code for CP components
    projects_dir = os.path.join(kafka_dir, "projects")
    if os.path.exists(projects_dir):
        shutil.rmtree(projects_dir)

    test_runner = kafka_runner(args, venv_dir)
    
    # download projects and install dependencies, but don't build yet (i.e. checkout only)
    reuse_image = args.aws and args.image_name
    build_scope = '' if (args.install_type == SOURCE_INSTALL and not reuse_image) else '--kafka-only'
    # build_cmd = "./build.sh --update --checkout-only {}".format(build_scope)
    # run(build_cmd, print_output=True, venv=False, allow_fail=False, cwd=kafka_dir)
    run(f"{args.python} -m pip install -U -r resources/requirements.txt",
        print_output=True, venv=True, allow_fail=False, cwd=kafka_dir)

    # run(f"{args.python} -m pip install .",
    #     print_output=True, venv_dir=venv_dir, venv=True, allow_fail=True,
    #     cwd=f"{kafka_dir}/projects/kafka/tests")
    # override dep versions for muckrake
    run(f"{args.python} -m pip install -U -r resources/requirement_override.txt",
        print_output=True, venv=True, allow_fail=True, cwd=kafka_dir)

    exit_status = 0

    
    try:
    # Check that the test path is valid before doing expensive cluster bringup
    # We still do this after the build step since that's how we get kafka, and our ducktape dependency
        test_path = " ".join(args.test_path)
        # cmd = f"{args.python} `which ducktape` {test_path} --collect-only"
        # run(cmd, venv=True, venv_dir=venv_dir, print_output=True, allow_fail=False)
        # if args.collect_only:
        #     # Nothing more to do
        #     logging.info("--collect-only flag used; exiting without running tests")
        #     return
        # Skip build if we are re-using an older image
        # if not reuse_image:
        #     build_cmd = f"./build.sh {build_scope}"
        #     run(build_cmd, print_output=True, venv=False, allow_fail=False, cwd=kafka_dir)

        image_id = None
        if args.aws:
            if args.image_name:
                image = image_from(name=args.image_name)
                if not image:
                    raise ValueError(f'{args.image_name} is not a valid AWS image name')
                image_id = image.image_id
            else:
                base_ami = AMI
                ssh_account = 'ubuntu'
            logging.info(f"linux distro input: {args.linux_distro}")
            logging.info(f"base_ami: {base_ami}")
        
        
        
    # Take down any existing to bring up cluster from scratch
        print("calling generate_tf_file")
        test_runner.generate_tf_file()
        test_runner.setup_tf_variables(base_ami)
        test_runner.destroy_terraform(allow_fail=True)
        
    

        cluster_file_name = f"{ABS_KAFKA_DIR}/tf-cluster.json"    
        if args.aws:
            # re-source vagrant credentials before bringing up cluster
            run(f". assume-iam-role arn:aws:iam::419470726136:role/semaphore-access; cd {kafka_dir};",
                print_output=True, allow_fail=False)
            test_runner.provission_terraform()
            print("calling function to generate cluster file")
            test_runner.generate_clusterfile()
        else:
            run(f"cd {kafka_dir};",
                print_output=True, allow_fail=False)
            test_runner.provission_terraform()
            test_runner.generate_clusterfile()
        if logging.getLogger().isEnabledFor(logging.DEBUG):
            with open(f"{kafka_dir}/tf-cluster.json", "r") as f:
                logging.debug(f'starting with cluster: {f.read()}')
        test_runner.wait_until_ready()

    except Exception as e:
        logging.warning(e)
        logging.warning(format_exc())
        exit_status = 1
    finally:
        # Cleanup
        if not args.collect_only and args.cleanup:
            logging.info("bringing down terraform cluster...")
            test_runner.destroy_terraform()

        elif not args.cleanup:
            logging.warning("--cleanup is false, leaving nodes alive")
        sys.exit(exit_status)


if __name__ == "__main__":
    main()