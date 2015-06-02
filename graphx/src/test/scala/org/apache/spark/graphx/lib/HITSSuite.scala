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

import org.scalatest.FunSuite

import org.apache.spark.graphx._
import org.apache.spark.graphx.util.GraphGenerators


object GridHITS {
  def apply(nRows: Int, nCols: Int, nIter: Int): Seq[(VertexId, (Double, Double))] = {
    val  inNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    val outNbrs = Array.fill(nRows * nCols)(collection.mutable.MutableList.empty[Int])
    // Convert row column address into vertex ids (row major order)
    def sub2ind(r: Int, c: Int): Int = r * nCols + c
    // Make the grid graph
    for (r <- 0 until nRows; c <- 0 until nCols) {
      val srcInd = sub2ind(r,c)
      if (r + 1 < nRows) {
        val dstInd = sub2ind(r + 1,c)
         inNbrs(dstInd) += srcInd
        outNbrs(srcInd) += dstInd
      }
      if (c + 1 < nCols) {
        val dstInd = sub2ind(r,c + 1)
         inNbrs(dstInd) += srcInd
        outNbrs(srcInd) += dstInd
      }
    }
    // Compute HITS scores
    var hits = Array.fill(nRows * nCols)((1.0, 1.0))
    var norm = 0.0
    for (iter <- 0 until nIter) {
      // Compute auth score
      for (ind <- 0 until (nRows * nCols)) {
        hits(ind) = (
          inNbrs(ind).map { nbr => hits(nbr) match { case (auth, hub) => hub }}.sum,
          hits(ind) match { case (auth, hub) => hub }
        )
      }
      norm = math.sqrt(hits.map { case (auth, hub) => auth * auth }.sum)
      hits = hits.map { case (auth, hub) => (auth / norm, hub) }
      // Compute hub score
      for (ind <- 0 until (nRows * nCols)) {
        hits(ind) = (
          hits(ind) match { case (auth, hub) => auth },
          outNbrs(ind).map { nbr => hits(nbr) match { case (auth, hub) => auth }}.sum
        )
      }
      norm = math.sqrt(hits.map { case (auth, hub) => hub * hub }.sum)
      hits = hits.map { case (auth, hub) => (auth, hub / norm) }
    }
    // Output
    (0L until (nRows * nCols)).zip(hits)
  }

}


class HITSSuite extends FunSuite with LocalSparkContext {

  def compareScores(a: VertexRDD[(Double, Double)], b: VertexRDD[(Double, Double)]): Double = {
    a.leftJoin(b) { case (id, a, bOpt) => (a, bOpt.getOrElse((0.0, 0.0))) }
     .map { case (id, ((a1, h1), (a2, h2))) => (a1 - a2) * (a1 - a2) + (h1 - h2) * (h1 - h2) }
     .sum
  }

  test("Star HITS") {
    withSpark { sc =>
      val nVertices = 100
      val starGraph = GraphGenerators.starGraph(sc, nVertices).cache()
      val errorTol = 1.0e-5

      val staticScores1 = starGraph.staticHITS(numIter = 1).vertices
      val staticScores2 = starGraph.staticHITS(numIter = 2).vertices.cache()

      // Static HITS should only take 2 iterations to converge
      val notMatching = staticScores1.innerZipJoin(staticScores2) {
        case (id, (a1, h1), (a2, h2)) => if ((a1 == a2) && (h1 == h2)) 0 else 1
      }.map { case (id, test) => test }.sum()
      assert(notMatching === 0)

      // Compare against exact HITS scores.
      val staticErrors = staticScores2.map { case (id, (auth, hub)) =>
        val p = math.abs(hub - 1.0 / math.sqrt(nVertices - 1))
        val correct = (id >  0  && auth == 0.0 && p < errorTol) ||
                      (id == 0L && auth == 1.0 && hub == 0.0)
        if (!correct) 1 else 0
      }
      assert(staticErrors.sum === 0)
    }
  } // end of test Star HITS

  test("Grid HITS") {
    withSpark { sc =>
      val rows = 10
      val cols = 10
      val numIter = 10
      val errorTol = 1.0e-5
      val gridGraph = GraphGenerators.gridGraph(sc, rows, cols).cache()

      val staticScores = gridGraph.staticHITS(numIter).vertices.cache()
      val referenceScores = VertexRDD(
        sc.parallelize(GridHITS(rows, cols, numIter))).cache()

      assert(compareScores(staticScores, referenceScores) < errorTol)
    }
  } // end of Grid HITS

  test("Chain HITS") {
    withSpark { sc =>
      val n = 9
      val chain1 = (0 until n).map(x => (x, x + 1))
      val rawEdges = sc.parallelize(chain1, 1).map { case (s, d) => (s.toLong, d.toLong) }
      val chain = Graph.fromEdgeTuples(rawEdges, 1.0).cache()
      val errorTol = 1.0e-5

      val staticScores1 = chain.staticHITS(numIter = 1).vertices
      val staticScores2 = chain.staticHITS(numIter = 2).vertices.cache()

      // Static HITS should only take 2 iterations to converge
      val notMatching = staticScores1.innerZipJoin(staticScores2) {
        case (id, (a1, h1), (a2, h2)) => if ((a1 == a2) && (h1 == h2)) 0 else 1
      }.map { case (id, test) => test }.sum()
      assert(notMatching === 0)

      // Compare against exact HITS scores.
      val staticErrors = staticScores2.map { case (id, (auth, hub)) =>
        val s = 1.0 / math.sqrt(n)
        val pa = math.abs(auth - s)
        val ph = math.abs(hub  - s)
        val correct = (0 < id && id < 9 && pa < errorTol && ph < errorTol) ||
                      (id == 0L && auth == 0.0 && ph < errorTol) ||
                      (id == 9L && pa < errorTol && hub == 0.0)
        if (!correct) 1 else 0
      }
      assert(staticErrors.sum === 0)
    }
  } // end of Chain HITS
}
