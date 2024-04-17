#!/bin/bash

# This script is used to install common dependencies for the base AMIs.
# As of now only keycloak dependency is added. There are other dependencies that will be moved here in future.
# Refer https://confluentinc.atlassian.net/browse/CPTF-364 for more details

MUCKRAKE_DEPS_S3_URL="https://s3-us-west-2.amazonaws.com/muckrake-dependencies"
KEYCLOAK_S3_DIR_NAME="keycloak"

function install_keycloak {
  # Version number is hardcoded because we support only one version of keycloak like other external dependencies such as elastic search, hive etc.
    KEYCLOAK_VERSION="23.0.4"
    KEYCLOAK_BINARY="keycloak-${KEYCLOAK_VERSION}.tar.gz"
    KEYCLOAK_DIR_NAME="keycloak-${KEYCLOAK_VERSION}"
    KEYCLOAK_SYMLINK_NAME="keycloak"

    pushd /opt/
    if [ ! -e ${KEYCLOAK_BINARY} ]; then
        pushd /tmp/vagrant-downloads
        if [ ! -e ${KEYCLOAK_BINARY} ]; then
            wget -nv "${MUCKRAKE_DEPS_S3_URL}/${KEYCLOAK_S3_DIR_NAME}/${KEYCLOAK_BINARY}"
        fi
        popd
        tar xzf /tmp/vagrant-downloads/${KEYCLOAK_BINARY}
    fi
    symlink /opt/${KEYCLOAK_DIR_NAME} /opt/${KEYCLOAK_SYMLINK_NAME}
    popd
    chmod a+rwx -R /opt/${KEYCLOAK_SYMLINK_NAME}
}