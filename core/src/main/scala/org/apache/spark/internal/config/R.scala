/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.internal.config

private[spark] object R {

  val R_BACKEND_CONNECTION_TIMEOUT = ConfigBuilder("spark.r.backendConnectionTimeout")
    .version("2.1.0")
    .intConf
    .createWithDefault(6000)

  val R_NUM_BACKEND_THREADS = ConfigBuilder("spark.r.numRBackendThreads")
    .version("1.4.0")
    .intConf
    .createWithDefault(2)

  val R_HEARTBEAT_INTERVAL = ConfigBuilder("spark.r.heartBeatInterval")
    .version("2.1.0")
    .intConf
    .createWithDefault(100)

  val SPARKR_COMMAND = ConfigBuilder("spark.sparkr.r.command")
    .version("1.5.3")
    .stringConf
    .createWithDefault("Rscript")

  val R_COMMAND = ConfigBuilder("spark.r.command")
    .version("1.5.3")
    .stringConf
    .createOptional

  /** The SparkR daemon will evaluate this before forking. */
  val R_DAEMON_INIT = ConfigBuilder("spark.r.daemonInit")
    .version("3.2.0")
    .stringConf
    .createWithDefault("NULL")

  /** milliseconds to wait until we give up launching the daemon */
  val R_DAEMON_TIMEOUT = ConfigBuilder("spark.r.daemonTimeout")
    .version("3.2.0")
    .intConf
    .createWithDefault(10000)

  /** Log Timing Info. Must be set to an R boolean expression. */
  val R_WORKER_VERBOSE = ConfigBuilder("spark.r.workerVerbose")
    .version("3.2.0")
    .stringConf
    .createWithDefault("FALSE")
}
