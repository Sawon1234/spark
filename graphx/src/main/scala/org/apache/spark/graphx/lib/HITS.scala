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

package org.apache.spark.graphx.lib

import scala.reflect.ClassTag
import scala.language.postfixOps

import org.apache.spark.Logging
import org.apache.spark.graphx._


/**
 * Hyperlink-Induced Topic Search (HITS) algorithm implementation ("hubs and authorities").
 *
 * This implementation uses the standalone [[Graph]] interface and runs HITS for a fixed
 * number of iterations:
 * {{{
 * var auth = Array.fill(n)( 1.0 )
 * var hub  = Array.fill(n)( 1.0 )
 * var norm = 0.0
 * for( iter <- 0 until numIter ) {
 *   for( i <- 0 until n ) {
 *     auth[i] = inNbrs[i].map(j => hub[j]).sum
 *   }
 *   norm = math.sqrt(auth.map(score => score * score).sum)
 *   auth = auth.map(score => score / norm)
 *   for( i <- 0 until n ) {
 *     hub[i] = outNbrs[i].map(j => auth[j]).sum
 *   }
 *   norm = math.sqrt(hub.map(score => score * score).sum)
 *   hub = hub.map(score => score / norm)
 * }
 * }}}
 */
object HITS extends Logging {


  /**
   * Run HITS for a fixed number of iterations returning a graph
   * with vertex attributes containing the HITS scores in the form
   * (authScore, hubScore) and empty edge attributes.
   *
   * @tparam VD the original vertex attribute (not used)
   * @tparam ED the original edge attribute (not used)
   *
   * @param graph the graph on which to compute HITS
   * @param numIter the number of iterations of HITS to run
   *
   * @return the graph with each vertex containing the HITS scores and empty edge attributes
   *
   */
  def run[VD: ClassTag, ED: ClassTag](
      graph: Graph[VD, ED], numIter: Int): Graph[(Double, Double), Unit] =
  {
    // Initialize the HITS graph with each vertex attribute as (1.0, 1.0).
    var scoreGraph: Graph[(Double, Double), Unit] = graph
      // Set the vertex attributes to the initial HITS scores.
      .mapVertices((id, attr) => (1.0, 1.0))
      // Clear edge attributes.
      .mapEdges(attr => {})

    var iteration = 0
    var prevScoreGraph: Graph[(Double, Double), Unit] = null
    var norm = 1.0

    while (iteration < numIter) {
      scoreGraph.cache()

      // Compute the authScore contributions of each vertex, perform local preaggregation, and do
      // the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      var authScores = scoreGraph.aggregateMessages[Double](
        ctx => ctx.sendToDst(ctx.srcAttr match { case (auth, hub) => hub}),
        _ + _, TripletFields.Src
      ).cache()

      // Compute normalization factor. (Use accumulator/broadcast instead?)
      norm = math.sqrt(authScores
        .map { case (id, score) => score }
        .map(score => score * score)
        .sum
      )

      // Normalize authScores. (Should this be cached?)
      authScores = authScores.mapValues(score => score / norm).cache()

      // Apply new authScores, using outerJoin to set scores of vertices that didn't receive a
      // message to 0.0. Requires a shuffle for broadcasting updated scores to the edge partitions.
      prevScoreGraph = scoreGraph
      scoreGraph = scoreGraph.outerJoinVertices(authScores) {
        case (id, (oldAuth, hub), scoreOpt) => scoreOpt match {
          case Some(auth) => (auth, hub)
          case None => (0.0, hub)
        }
      }.cache()
      scoreGraph.edges.foreachPartition(x => {}) // also materializes scoreGraph.vertices
      authScores.unpersist()
      prevScoreGraph.unpersist()

      // Compute the hubScore contributions of each vertex, perform local preaggregation, and do
      // the final aggregation at the receiving vertices. Requires a shuffle for aggregation.
      var hubScores = scoreGraph.aggregateMessages[Double](
        ctx => ctx.sendToSrc(ctx.dstAttr match { case (auth, hub) => auth }),
        _ + _, TripletFields.Dst
      ).cache()

      // Compute normalization factor. (Use accumulator/broadcast instead?)
      norm = math.sqrt(hubScores
        .map { case (id, score) => score }
        .map(score => score * score)
        .sum
      )

      // Normalize hubScores. (Should this be cached?)
      hubScores = hubScores.mapValues(score => score / norm).cache()

      // Apply new hubScores, using outerJoin to set scores of vertices that didn't receive a
      // message to 0.0. Requires a shuffle for broadcasting updated scores to the edge partitions.
      prevScoreGraph = scoreGraph
      scoreGraph = scoreGraph.outerJoinVertices(hubScores) {
        case (id, (auth, oldHub), scoreOpt) => scoreOpt match {
          case Some(hub) => (auth, hub)
          case None => (auth, 0.0)
        }
      }.cache()
      scoreGraph.edges.foreachPartition(x => {}) // also materializes scoreGraph.vertices
      hubScores.unpersist()
      prevScoreGraph.unpersist()

      logInfo(s"HITS finished iteration $iteration.")

      iteration += 1
    }

    scoreGraph
  }
}
