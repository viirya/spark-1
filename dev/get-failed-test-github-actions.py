#!/usr/bin/env python3

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

from argparse import ArgumentParser

modules_target = {
  "core": "core/target/test-reports/",
  "unsafe": "core/target/test-reports/",
  "kvstore": "core/target/test-reports/,
  "avro": "core/target/test-reports/",
  "network-common": "core/target/test-reports/",
  "network-shuffle": "core/target/test-reports/",
  "repl": "core/target/test-reports/",
  "launcher": "core/target/test-reports/",
  "examples": "core/target/test-reports/",
  "sketch": "core/target/test-reports/",
  "graphx": "core/target/test-reports/",
  "catalyst": "core/target/test-reports/",
  "hive-thriftserver": "core/target/test-reports/",
  "streaming": "core/target/test-reports/",
  "sql-kafka-0-10": "core/target/test-reports/",
  "streaming-kafka-0-10": "core/target/test-reports/",
  "mllib-local": "core/target/test-reports/",
  "mllib": "core/target/test-reports/",
  "yarn": "core/target/test-reports/",
  "mesos": "core/target/test-reports/",
  "kubernetes": "core/target/test-reports/",
  "hadoop-cloud": "core/target/test-reports/",
  "spark-ganglia-lgpl": "core/target/test-reports/",
  "hive": "core/target/test-reports/",
  "sql": "core/target/test-reports/"
}

def parse_opts():
    parser = ArgumentParser(
        prog="get-failed-test-github-actions"
    )
    parser.add_argument(
        "-m", "--modules", type=str,
        default=None,
        help="A comma-separated list of modules to process on"
             "(default: %s)" % ",".join(sorted([m.name for m in modules.all_modules]))
    )

    args, unknown = parser.parse_known_args()
    if unknown:
        parser.error("Unsupported arguments: %s" % ' '.join(unknown))
    return args

def parse_failed_testcases_from_module(module):
    if module in modules_target:
        test_report_path = modules_target
        print("[info] Parsing test report for module: ", module, " from path: " + test_report_path)

    else:
        print("[error] Cannot get the test report path for module: ", module)

def main():
    opts = parse_opts()
    # Ensure the user home directory (HOME) is valid and is an absolute directory
    if not USER_HOME or not os.path.isabs(USER_HOME):
        print("[error] Cannot determine your home directory as an absolute path;",
              " ensure the $HOME environment variable is set properly.")
        sys.exit(1)

    os.chdir(SPARK_HOME)

if __name__ == "__main__":
    main()
