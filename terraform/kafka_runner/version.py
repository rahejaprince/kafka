# Copyright 2016 Confluent Inc.


from distutils.version import LooseVersion
from enum import Enum
from typing import List

CP_CURRENT_VERSION_STR = "7.8.0"

CP_KRAFT_SUPPORT_MIN_VERSION_STR = "7.4.0"


class ProjectVersion(LooseVersion):
    """Container for project versions which makes versions simple to compare.

    Examples where we may want to eventually use versions
    - non Confluent Platform projects, such as Hadoop
    - Confluent Platform
    """
    pass


class OS(Enum):
    CENTOS_7x = "CentOS 7.x"
    CENTOS_8x = "CentOS 8.x"
    DEBIAN_8 = "Debian 8"
    DEBIAN_9 = "Debian 9"
    UBUNTU_16_04 = "Ubuntu 16.04"
    UBUNTU_18_04 = "Ubuntu 18.04"


class ConfluentPlatformVersion(ProjectVersion):
    """Container for Confluent Platform version

    This has a concept of current version. current version is the "version under test"
    https://docs.confluent.io/platform/current/installation/versions-interoperability.html

    """

    def __init__(self,
                 vstring: str,
                 inter_broker_protocol_version: str = None,
                 log_message_format_version: str = None,
                 kafka_version: str = None,
                 cp_package_name: str = None,
                 supported_os: list = List,
                 support_java_versions: list = List):

        ProjectVersion.__init__(self, vstring)
        self.is_dev = vstring.lower() == "dev" or vstring == CP_CURRENT_VERSION_STR
        self._inter_broker_protocol_version = inter_broker_protocol_version
        self._log_message_format_version = log_message_format_version
        self._kafka_version = kafka_version
        self._cp_package_name = cp_package_name
        self._supported_os = supported_os
        self._supported_java_version = support_java_versions

        self.is_current = vstring.lower() == CP_CURRENT_VERSION_STR.lower()

    def __str__(self):
        if self.is_dev:
            return 'dev'
        else:
            return super().__str__()

    def _cmp(self, other):
        if isinstance(other, str):
            other = ConfluentPlatformVersion(other)

        if other.is_dev:
            if self.is_dev:
                return 0
            return -1
        elif self.is_dev:
            return 1

        return LooseVersion._cmp(self, other)

    def supports_named_listeners(self):
        return self >= CP_3_3

    def topic_command_supports_bootstrap_server(self):
        return self >= CP_5_3

    def topic_command_supports_if_not_exists_with_bootstrap_server(self):
        return self >= CP_6_0

    def kafka_configs_command_uses_bootstrap_server(self):
        # everything except User SCRAM Credentials (KIP-554)
        return self >= CP_6_0

    def kafka_configs_command_uses_bootstrap_server_scram(self):
        # User SCRAM Credentials (KIP-554)
        return self >= CP_6_1

    def supports_tls_to_zookeeper(self):
        # indicate if KIP-515 is available
        return self >= CP_5_5

    def consumer_supports_bootstrap_server(self):
        """
        Kafka supported a new consumer beginning with v0.9.0 where
        we can specify --bootstrap-server instead of --zookeeper.

        This version also allowed a --consumer-config file where we could specify
        a security protocol other than PLAINTEXT.

        CP 2.0.x  has kafka 0.9.0. Almost all supported CP versions should retrun true for this

        :return: true if the version of Kafka supports a new consumer with --bootstrap-server
        """
        return self >= CP_3_3

    def get_inter_broker_protocol_version(self):
        return self._inter_broker_protocol_version

    def get_log_message_format_version(self):
        return self._log_message_format_version

    def get_kafka_version(self):
        return self._kafka_version
    
    def get_package_name(self):
        return self._cp_package_name if self._cp_package_name else "confluent-platform"
    
    def get_supported_os(self):
        return self._supported_os

    def get_supported_java_versions(self):
        return self._supported_java_version


CP_3_3 = ConfluentPlatformVersion("3.3.3")

CP_5_3 = ConfluentPlatformVersion("5.3.7",
                                  inter_broker_protocol_version="2.3",
                                  log_message_format_version="2.3",
                                  kafka_version="2.3.0",
                                  cp_package_name="confluent-platform-2.12",
                                  supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_60", "11.0_2"])

CP_5_4 = ConfluentPlatformVersion(vstring="5.4.6",
                                  inter_broker_protocol_version="2.4",
                                  log_message_format_version="2.4",
                                  kafka_version="2.4.0",
                                  cp_package_name="confluent-platform-2.12",
                                  supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])

CP_5_5 = ConfluentPlatformVersion(vstring="5.5.7",
                                  inter_broker_protocol_version="2.5",
                                  log_message_format_version="2.5",
                                  kafka_version="2.5.0",
                                  cp_package_name="confluent-platform-2.12",
                                  supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])

CP_6_0 = ConfluentPlatformVersion(vstring="6.0.5",
                                  inter_broker_protocol_version="2.6",
                                  log_message_format_version="2.6",
                                  kafka_version="2.6.0",
                                  cp_package_name="confluent-platform",
                                  supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])

# Need this for comparison while constructing the release binary URL, as versions before CP 6.0.0 have scala version present in URL
CP_6_0_0 = ConfluentPlatformVersion(vstring="6.0.0",
                                  inter_broker_protocol_version="2.6",
                                  log_message_format_version="2.6",
                                  kafka_version="2.6.0",
                                  cp_package_name="confluent-platform",
                                  supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])

CP_6_1 = ConfluentPlatformVersion(vstring="6.1.4",
                                  inter_broker_protocol_version="2.7",
                                  log_message_format_version="2.7",
                                  kafka_version="2.7.0",
                                  cp_package_name="confluent-platform",
                                  supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])

CP_6_2 = ConfluentPlatformVersion(vstring="6.2.2",
                                  inter_broker_protocol_version="2.8",
                                  log_message_format_version="2.8",
                                  kafka_version="2.8.0",
                                  cp_package_name="confluent-platform",
                                  supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])

CP_7_0 = ConfluentPlatformVersion(vstring="7.0.0",
                                  inter_broker_protocol_version="3.0",
                                  log_message_format_version="3.0",
                                  kafka_version="3.0.0",
                                  cp_package_name="confluent-platform",
                                  supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])

CP_7_4 = ConfluentPlatformVersion(vstring="7.4.0",
                                  inter_broker_protocol_version="3.4",
                                  log_message_format_version="3.4",
                                  kafka_version="3.4.0",
                                  cp_package_name="confluent-platform",
                                  supported_os=[OS.CENTOS_8x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])

# Need for comparison for kafka 3.4.0, should be updated when 7.5 is released
CP_7_5 = ConfluentPlatformVersion(vstring="7.5.0",
                                  inter_broker_protocol_version=None,
                                  log_message_format_version=None,
                                  kafka_version="3.5.0",
                                  cp_package_name="confluent-platform",
                                  supported_os=[OS.CENTOS_8x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                OS.UBUNTU_18_04],
                                  support_java_versions=["1.8.0_202", "11.0_4"])


CP_CURRENT_VERSION = ConfluentPlatformVersion(vstring=CP_CURRENT_VERSION_STR,
                                              inter_broker_protocol_version=None,
                                              log_message_format_version=None,
                                              kafka_version="3.1.0",
                                              cp_package_name="confluent-platform",
                                              supported_os=[OS.CENTOS_7x, OS.DEBIAN_8, OS.DEBIAN_9, OS.UBUNTU_16_04,
                                                            OS.UBUNTU_18_04],
                                              support_java_versions=["1.8.0_202", "11.0_4"])

ALL_CP_VERSIONS = (
    CP_5_5,
    CP_6_0,
    CP_6_1,
    CP_6_2,
    CP_7_0,
    CP_CURRENT_VERSION
)