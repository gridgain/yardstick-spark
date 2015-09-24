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

package org.yardstick.spark.util

import org.apache.ignite.spark.IgniteContext

/**
 * IgniteUtils
 *
 */
object IgniteUtils {

  def waitForIgnite(ic: IgniteContext[_,_]) = {
    val cluster = ic.ignite.cluster
    val nServers = System.getenv("ignite_servers_count").toInt
    var curServers = 0
    var nloops = 0
    do {
      Thread.sleep(1000)
      val curTop = cluster.topology(cluster.topologyVersion)
      val curServers = curTop.size
      if (nloops % 5 == 0) {
        println(s"Waiting for $nServers Ignite Servers to come online: we now have $curServers ..")
      }
      nloops += 1
    } while (curServers < nServers)
  }
}
