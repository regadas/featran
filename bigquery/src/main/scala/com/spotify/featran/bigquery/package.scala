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

package com.spotify.featran

import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}

import scala.collection.JavaConverters._
import scala.language.higherKinds

package object bigquery {
  /**
   * [[FeatureBuilder]] for output as BigQuery `TableRow` type.
   */
  implicit def bigQueryFeatureBuilder: FeatureBuilder[TableRow] = new FeatureBuilder[TableRow] {
    private var tr: TableRow = _
    override def init(dimension: Int): Unit = tr = new TableRow
    override def add(name: String, value: Double): Unit = tr.set(name, value)
    override def skip(): Unit = Unit
    override def skip(n: Int): Unit = Unit
    override def result: TableRow = tr
  }

  /**
   * Enhanced version of [[FeatureExtractor]] with BigQuery methods.
   */
  implicit class BigQueryFeatureExtractor[M[_]: CollectionType, T]
  (val self: FeatureExtractor[M, T]) {
    def schema: M[TableSchema] = {
      val dt: CollectionType[M] = implicitly[CollectionType[M]]
      import dt.Ops._
      self.featureNames.map { xs =>
        val fields = xs.map { x =>
          new TableFieldSchema()
            .setName(x)
            .setMode("NULLABLE")
            .setType("FLOAT64")
        }
        new TableSchema().setFields(fields.asJava)
      }
    }
  }
}
