package com.learning.stream

import base.TestBootstrap

class FileStreamerTest extends TestBootstrap {
  private val DATASET_DIRECTORY="file:///tmp/spark/data"

  private var streamer: FileStreamer = _

  before {
    streamer = new FileStreamer()
  }

  it should "calculate weekly averages of aggregated counts of each customer for given week number and duration" in {
    //TRICK TO RUN: update existing file and creat a copy of it
    streamer.streamFrom(DATASET_DIRECTORY)
  }

}
