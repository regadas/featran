/*
 * Copyright 2018 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.spotify.featran.bigquery

import com.google.api.services.bigquery.model.TableRow
import com.spotify.featran.FeatureBuilder
import org.scalacheck._

object BigQueryFeatureBuilderSpec extends Properties("BigQueryFeatureBuilder") {

  private def list[T](implicit arb: Arbitrary[Option[T]]): Gen[List[Option[T]]] =
    Gen.listOfN(100, arb.arbitrary)

  property("BigQuery TableRow") = Prop.forAll(list[Double]) { xs =>
    val fb = implicitly[FeatureBuilder[TableRow]]
    fb.init(xs.size + 4)
    val expected = new TableRow
    xs.zipWithIndex.foreach {
      case (Some(x), i) =>
        val key = "key" + i.toString
        fb.add(key, x)
        expected.set(key, x)
      case (None, _) => fb.skip()
    }
    fb.add(Iterable("x", "y"), Seq(0.0, 0.0))
    fb.skip(2)
    val actual = fb.result
    expected.set("x", 0.0)
    expected.set("y", 0.0)
    actual == expected
  }

}
