package com.learning.stream

import base.TestBootstrap
import base.TestSetup.kill

class FileStreamerTest extends TestBootstrap {
  private val DATASET_DIRECTORY="file:///tmp/spark/data"

  private var streamer: FileStreamer = _

  before {
    streamer = new FileStreamer()
  }

  ignore should "calculate weekly averages of aggregated counts of each customer for given week number and duration" in {
    //TRICK TO RUN: update existing file mentioned dir and create a copy of it
    streamer.streamFrom(DATASET_DIRECTORY)
  }

  after {
    kill
  }
}
