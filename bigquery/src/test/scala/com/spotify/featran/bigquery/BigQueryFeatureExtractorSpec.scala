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

import com.google.api.services.bigquery.model.{TableFieldSchema, TableSchema}
import com.spotify.featran.FeatureSpec
import com.spotify.featran.transformers.Identity
import org.scalacheck._

import scala.collection.JavaConverters._

object BigQueryFeatureExtractorSpec extends Properties("BigQueryFeatureExtractor") {

  private implicit val arbNames: Arbitrary[List[String]] = Arbitrary {
    Gen.nonEmptyListOf(Gen.alphaStr.filter(_.nonEmpty))
  }

  property("TableSchema") = Prop.forAll { xs: List[String] =>
    val fs = xs.foldLeft(FeatureSpec.of[Double])((f, x) => f.required(identity)(Identity(x)))
    val fields = xs.map { x =>
      new TableFieldSchema()
        .setName(x)
        .setMode("NULLABLE")
        .setType("FLOAT64")
    }
    val expected = new TableSchema().setFields(fields.asJava)
    fs.extract(Seq(1.0)).schema.head == expected
  }

}
