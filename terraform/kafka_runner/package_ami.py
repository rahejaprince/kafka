
# Copyright 2016 Confluent Inc.

import glob
import hashlib
import logging
import os
import re
import time
from datetime import datetime, timedelta

import boto3

from terraform.kafka_runner.version import ALL_CP_VERSIONS, CP_6_0_0
from terraform.kafka_runner.util import HASH_ALGORITHM, AWS_REGION, AWS_ACCOUNT_ID, AMI_NAME_MAX_LENGTH, AMI, \
    BASE_KAFKA_DIR, AWS_PACKER_JSON, run, WORKER_AMI_JSON, WORKER_AMI_NAME, INSTANCE_TYPE, ARM_AWS_PACKER_JSON, ARM_WORKER_AMI_JSON

GET_VERSION_FROM_PACKAGES_RE = re.compile('.*confluent-packages-(?P<major>\d+)\.(?P<minor>\d+)\.(?P<patch>\d+).*')

def hash_files(file_list, **kwargs):
    """
    Creates a hash based on the contents of the files.

    Arguments:
      file_list: a list of file paths
      **kwargs: additional key-value pairs to include in the hash
    """
    hasher = hashlib.new(HASH_ALGORITHM)
    sorted_files = sorted(file_list)

    hasher.update(str(sorted(kwargs.items())).encode())

    for f in sorted_files:
        with open(f, "rb") as fd:
            hasher.update(fd.read())

    return hasher.hexdigest()


def ensure_trailing_separator(dirname):
    """Ensure trailing separator on the directory name

    E.g::
        my/path -> my/path/   # Add trailing '/'
        my/path/ -> my/path/  # No change
    """
    if not dirname.endswith(os.path.sep):
        dirname += os.path.sep
    return dirname


def compute_packer_hash(**extras):
    """Compute a hash which depends on the contents and directory layout of Packer files.

    Since Packer files are changed infrequently, hopefully this provides a reasonable way to cache and reuse a
    pre-created ami.

    Arguments:
      **extras: named user arguments to pass to packer
    """
    previous_wd = os.getcwd()
    os.chdir(BASE_KAFKA_DIR)

    dirname = os.path.dirname(AWS_PACKER_JSON)

    def with_extension(extension):
        return glob.glob(os.path.join(dirname, '*.%s' % extension))

    file_list = with_extension('sh') + with_extension('json') + [
        os.path.join(BASE_KAFKA_DIR, "resources/requirements.txt")]

    logging.info('Files considered for packer_hash: %s', ', '.join(file_list))
    logging.info('Extras considered for packer_hash: %s', extras)

    hash_val = hash_files(file_list, **extras)
    os.chdir(previous_wd)

    return hash_val


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


def create_ami(image_name, source_ami=AMI, region_name=AWS_REGION, volume_size=50,
               packer_json=AWS_PACKER_JSON, instance_type=INSTANCE_TYPE, **extras):
    """Create a new ami using packer!"""
    previons_wd = os.getcwd()
    os.chdir(BASE_KAFKA_DIR)
    extras.setdefault('linux_distro', os.environ.get('LINUX_DISTRO', 'ubuntu'))

    cmd = 'packer build'
    cmd += ' -var "region=%s"' % region_name
    cmd += ' -var "source_ami=%s"' % source_ami
    cmd += ' -var "ami_name=%s"' % image_name
    cmd += ' -var "volume_size=%s"' % volume_size
    cmd += ' -var "instance_type=%s"' % instance_type
    cmd += ''.join([' -var "{}={}"'.format(*v) for v in extras.items() if v[1] is not None])
    cmd += ' ' + packer_json

    logging.info("Creating a new image with name %s in region %s..." % (image_name, region_name))
    logging.info("   This may take 10-20 minutes...")
    run(cmd, allow_fail=False, print_output=True)

    image = image_from(name=image_name, region_name=region_name)
    assert image is not None, "Expected aws image %s to exist after running packer!" % image_name
    os.chdir(previons_wd)

    logging.info('Successfully created new image with id = %s', image.image_id)

    return image


def wait_ready(image_id, region_name=AWS_REGION, timeout_sec=600):
    """Block until the given image_id is ready. Raise exception if no image with the given id."""

    logging.info("Waiting for %s to become available..." % image_id)
    start = time.time()
    backoff = 5
    counter = 0
    while time.time() - start <= timeout_sec:
        image = image_from(image_id=image_id, region_name=region_name)
        assert image is not None, "Expected an image to exist with id %s, but it doesn't." % image_id

        if image.state.lower() == "available":
            logging.info("Image %s is available." % image_id)
            break
        time.sleep(backoff)
        counter += 1

        # progress bar, indicate + for each minute elapsed
        if counter % (60 / backoff) == 0:
            print("+")
        else:
            print("-")


def package_base_ami(instance_type=INSTANCE_TYPE, source_ami=AMI, ssh_account=None, volume_size=50, arm_image=False, **hash_extras):
    if ssh_account is None:
        ssh_account = "ubuntu"
    packer_hash = compute_packer_hash(source_ami=source_ami, **hash_extras)
    logging.info("packer_hash: %s" % packer_hash)

    ami_name = "muckrake-%s" % packer_hash
    ami_name = ami_name[:AMI_NAME_MAX_LENGTH]  # Truncate to maximum length
    logging.info("Base AMI name: %s (created from %s)" % (ami_name, source_ami))

    # Check for cached image, and create if not present
    image = image_from(name=ami_name)
    if image:
        logging.info("Found image matching %s: %s" % (ami_name, image))
        # Corner case: wait until image is ready
        wait_ready(image.image_id)
    else:
        logging.info("No image matching %s." % ami_name)
        image = create_ami(ami_name, instance_type=instance_type, source_ami=source_ami, ssh_account=ssh_account, volume_size=volume_size, packer_json=ARM_AWS_PACKER_JSON if arm_image else AWS_PACKER_JSON, **hash_extras)

    return image.image_id


def get_cp_version(resource_url):
    match = GET_VERSION_FROM_PACKAGES_RE.match(resource_url)
    return f"{match.group('major')}.{match.group('minor')}.{match.group('patch')}" if match else None


def get_package_name(resource_url):
    """
    We need to resolve CP package name here as older version has scala version along with cp version
    """

    version = get_cp_version(resource_url)
    for cp_version in ALL_CP_VERSIONS:
        if cp_version.vstring == version:
            return cp_version.get_package_name()

    if cp_version < CP_6_0_0:
        return "confluent-platform-2.12"

    return "confluent-platform"


def get_resource_url(install_type, resource_url):
    """
    Complete a resource url for archive type if it doesn't have package name. 
    Package name gets rsolved here based on the CP version. 
    """
    tar_suffix = ".tar.gz"
    # if install type is not tarball, return
    if install_type != 'tarball' and install_type != 'version_tarball':
        return resource_url

    # ensure url has package name
    if resource_url.endswith(tar_suffix):
        return resource_url

    # append the package name at the end of the url 
    return f"{resource_url}/{get_package_name(resource_url)}{tar_suffix}"


def package_worker_ami(install_type, volume_size, source_ami=AMI, resource_url=None,
                       instance_type=INSTANCE_TYPE, ssh_account=None, arm_image=False, **extras):
    """ Create a worker AMI with Confluent Platform """
    if ssh_account is None:
        ssh_account = "ubuntu"
    base_ami = package_base_ami(instance_type=instance_type, source_ami=source_ami, ssh_account=ssh_account,
                                volume_size=volume_size, arm_image=arm_image, **extras)

    logging.info("Worker AMI name: %s" % WORKER_AMI_NAME)

    image = create_ami(WORKER_AMI_NAME, source_ami=base_ami, packer_json=ARM_WORKER_AMI_JSON if arm_image else WORKER_AMI_JSON, install_type=install_type,
                       ssh_account=ssh_account, volume_size=volume_size, instance_type=instance_type,
                       resource_url=get_resource_url(install_type, resource_url),
                       cp_package_name=get_package_name(resource_url), **extras)

    delete_old_worker_amis()

    return image.image_id


def delete_old_worker_amis():
    """ Delete worker AMIs older than 30 days """
    logging.info('Checking for old worker AMIs to delete...')

    ec2 = boto3.resource("ec2", region_name=AWS_REGION)
    for image in ec2.images.filter(Owners=[AWS_ACCOUNT_ID], Filters=[{'Name': 'tag:Service', 'Values': ['Muckrake']},
                                                                     {'Name': 'tag:Type', 'Values': ['Worker']}]):
        created_date = datetime.strptime(image.creation_date, "%Y-%m-%dT%H:%M:%S.000Z")

        if datetime.utcnow() - created_date > timedelta(days=30):
            snapshot_ids = [s['Ebs']['SnapshotId'] for s in image.block_device_mappings if 'Ebs' in s]
            logging.info('Deleting worker AMI {} with snapshot(s): {}'.format(image.id, snapshot_ids))

            image.deregister()
            for snapshot in ec2.snapshots.filter(SnapshotIds=snapshot_ids):
                snapshot.delete()
