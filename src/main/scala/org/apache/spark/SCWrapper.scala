package org.apache.spark

import org.apache.hadoop.conf.Configuration
import org.apache.spark.util.SerializableConfiguration

class SCWrapper(conf: Configuration) extends SerializableConfiguration(conf: Configuration) {}
