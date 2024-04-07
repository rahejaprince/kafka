PYTHON=${PYTHON:-python3.7}

$PYTHON -m pip install --upgrade pip
echo "Retriving all the dependencies"
$PYTHON -m pip install -r $KAFKA_DIR/terraform/resources/requirements.txt
echo "calling run_test file"
#packer --version


$PYTHON -m terraform.kafka_runner.run_tests ${TEST_PATH=$KAFKA_DIR/tests/kafkatest/tests/core/security_test.py} \
    --aws \
    --num-workers ${NUM_WORKERS:-10} \
    --install-type ${INSTALL_TYPE:-source} \
    --worker-instance-type ${WORKER_INSTANCE_TYPE:-'c4.xlarge'} \
    --worker-volume-size ${WORKER_VOLUME_SIZE:-40} \
    --cleanup ${CLEANUP_INSTANCES:-true} \
    --resource-url ${RESOURCE_URL:-''} \
    --results-root $RESULTS \
    --repeat ${REPEAT:-1} \
    --linux-distro ${LINUX_DISTRO} \
    --jdk-version ${DEFAULT_JDK_VERSION:-8} \
    --python ${PYTHON} \
    --build-url ${BUILD_URL} \
    ${NEW_GLOBALS} \
    $SPOT_INSTANCE_FLAGS \
    $ADDITIONAL_FLAGS \
    $SAMPLE_ARG \
    $DUCKTAPE_ARGS