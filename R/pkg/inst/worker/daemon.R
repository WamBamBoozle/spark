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

# Logging
# -------
#
# stdout and stderr from this script is copied to the spark logs at INFO level.
# Since spark normally runs at log level 'WARN', if you want to see output from
# this script, add the line
#
#. setLogLevel('INFO')
#
# then you should see output like
#
# 2020-08-11 16:16:35 INFO  RRunner:54 - Times: boot = 0.835 s, \
# init = 0.005 s, broadcast = 0.001 s, read-input = 0.010 s, \
# compute = 0.001 s, write-output = 1.163 s, total = 2.015 s

# Worker daemon

rLibDir <- Sys.getenv("SPARKR_RLIBDIR")
connectionTimeout <- as.integer(Sys.getenv("SPARKR_BACKEND_CONNECTION_TIMEOUT", "6000"))
dirs <- strsplit(rLibDir, ",")[[1]]
script <- file.path(dirs[[1]], "SparkR", "worker", "worker.R")

Log <- function(...){
  message("daemon.R: ", ...)
}

source(script)

# preload SparkR package, speedup worker
.libPaths(c(dirs, .libPaths()))
suppressPackageStartupMessages(library(SparkR))

port <- as.integer(Sys.getenv("SPARKR_WORKER_PORT"))
inputCon <- socketConnection(
    port = port, open = "wb", blocking = TRUE, timeout = connectionTimeout)

SparkR:::doServerAuth(inputCon, Sys.getenv("SPARKR_WORKER_SECRET"))

# Waits indefinitely for a socket connection by default.
selectTimeout <- NULL

MaybePrintNull <- function(n) if (is.null(n)) 'NULL' else n

process <- function() {
  ready <- socketSelect(list(inputCon), timeout = selectTimeout)

  # Note that the children should be terminated in the parent. If each child terminates
  # itself, it appears that the resource is not released properly, that causes an unexpected
  # termination of this daemon due to, for example, running out of file descriptors
  # (see SPARK-21093). Therefore, the current implementation tries to retrieve children
  # that are exited (but not terminated) and then sends a kill signal to terminate them properly
  # in the parent.
  #
  # There are two paths that it attempts to send a signal to terminate the children in the parent.
  #
  #   1. Every second if any socket connection is not available and if there are child workers
  #     running.
  #   2. Right after a socket connection is available.
  #
  # In other words, the parent attempts to send the signal to the children every second if
  # any worker is running or right before launching other worker children from the following
  # new socket connection.

  # The process IDs of exited children are returned below.
  children <- parallel:::selectChildren(timeout = 0)

  if (is.integer(children)) {
    lapply(children, function(child) {
      # This should be the PIDs of exited children. Otherwise, this returns raw bytes if any data
      # was sent from this child. In this case, we discard it.
      pid <- parallel:::readChild(child)
      if (is.integer(pid)) {
        # This checks if the data from this child is the same pid of this selected child.
        if (child == pid) {
          # If so, we terminate this child.
          tools::pskill(child, tools::SIGUSR1)
        }
      }
    })
  } else if (is.null(children)) {
    # If it is NULL, there are no children. Waits indefinitely for a socket
    # connection.
    selectTimeout <- NULL
  }

  if (ready) {
    port <- SparkR:::readInt(inputCon)
    # There is a small chance that it could be interrupted by signal, retry
    # one time
    if (length(port) == 0) {
      port <- SparkR:::readInt(inputCon)
      if (length(port) == 0) {
        Log("quitting daemon")
        quit(save = "no")
      }
    }
    selectTimeout <<-
      MaybeForkedWorker(selectTimeout, inputCon, port)
  }
}

DieNoisily <- function(condition) {
  Log(condition, paste0("\n", capture.output(traceback()), "\n"))
  quit(status = 1, save = "no")
}

while (TRUE)
  tryCatch(process(), warning = DieNoisily, error = DieNoisily)
