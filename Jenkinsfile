#!/usr/bin/env groovy

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

def config = jobConfig {
    cron = '@midnight'
    nodeLabel = 'docker-oraclejdk8'
    testResultSpecs = ['junit': '**/build/test-results/**/TEST-*.xml']
    slackChannel = '#kafka-warn'
    timeoutHours = 4
    runMergeCheck = false
}

def job = {
    // https://github.com/confluentinc/common-tools/blob/master/confluent/config/dev/versions.json
    def kafkaMuckrakeVersionMap = [
            "2.4": "5.4.x",
            "2.3": "5.3.x",
            "trunk": "master",
            "master": "master"
    ]

    // Per KAFKA-7524, Scala 2.12 is the default, yet we currently support the previous minor version.
    stage("Check compilation compatibility with Scala 2.11") {
        sh "gradle"
        sh "./gradlew clean compileJava compileScala compileTestJava compileTestScala " +
                "--stacktrace -PscalaVersion=2.11"
    }

    stage("Compile and validate") {
        sh "./gradlew clean assemble spotlessScalaCheck checkstyleMain checkstyleTest spotbugsMain " +
                "--no-daemon --stacktrace --continue -PxmlSpotBugsReport=true"
    }

    if (config.publish && config.isDevJob) {
      withGradleFile([["gradle/artifactory_snapshots_settings", "settings_file", "${env.WORKSPACE}/init.gradle", "GRADLE_NEXUS_SETTINGS"]]) {
          stage("Publish to artifactory") {
              sh "./gradlew --init-script ${GRADLE_NEXUS_SETTINGS} --no-daemon uploadArchivesAll"
          }
      }
    }

    stage("Run Tests and build cp-downstream-builds") {
        def testTargets = [
                "Step run-tests"           : {
                    echo "Running unit and integration tests"
                    sh "./gradlew unitTest integrationTest " +
                            "--no-daemon --stacktrace --continue -PtestLoggingEvents=started,passed,skipped,failed -PmaxParallelForks=4 -PignoreFailures=true"
                },
                "Step cp-downstream-builds": {
                    echo "Building cp-downstream-builds"
                    if (config.isPrJob) {
                        def muckrakeBranch = kafkaMuckrakeVersionMap[env.CHANGE_TARGET]
                        def forkRepo = "${env.CHANGE_FORK ?: "confluentinc"}/kafka.git"
                        def forkBranch = env.CHANGE_BRANCH
                        echo "Schedule test-cp-downstream-builds with :"
                        echo "Muckrake branch : ${muckrakeBranch}"
                        echo "PR fork repo : ${forkRepo}"
                        echo "PR fork branch : ${forkBranch}"
                        buildResult = build job: 'test-cp-downstream-builds', parameters: [
                                [$class: 'StringParameterValue', name: 'BRANCH', value: muckrakeBranch],
                                [$class: 'StringParameterValue', name: 'TEST_PATH', value: "muckrake/tests/dummy_test.py"],
                                [$class: 'StringParameterValue', name: 'KAFKA_REPO', value: forkRepo],
                                [$class: 'StringParameterValue', name: 'KAFKA_BRANCH', value: forkBranch]],
                                propagate: true, wait: true
                    }
                }
        ]

        parallel testTargets
        return null
    }
}

def post = {
    stage('Upload results') {
        // Kafka failed test stdout files
        archiveArtifacts artifacts: '**/testOutput/*.stdout', allowEmptyArchive: true

        def summary = junit '**/build/test-results/**/TEST-*.xml'
        def total = summary.getTotalCount()
        def failed = summary.getFailCount()
        def skipped = summary.getSkipCount()
        summary = "Test results:\n\t"
        summary = summary + ("Passed: " + (total - failed - skipped))
        summary = summary + (", Failed: " + failed)
        summary = summary + (", Skipped: " + skipped)
        return summary;
    }
}

runJob config, job, post
