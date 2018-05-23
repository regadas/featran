/*
 * Copyright 2017 Spotify AB.
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

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.language.{higherKinds, implicitConversions}
import scala.reflect.ClassTag

/**
 * Type class for collections to extract features from.
 * @tparam M collection type
 */
trait CollectionType[M[_]] { self =>
  def pure[A: ClassTag](ma: M[_], a: A): M[A]

  def map[A, B: ClassTag](ma: M[A], f: A => B): M[B]

  def reduce[A](ma: M[A], f: (A, A) => A): M[A]

  def cross[A, B: ClassTag](ma: M[A], mb: M[B]): M[(A, B)]

  class MOps[A](ma: M[A]) {
    def pure[B: ClassTag](b: B): M[B] = self.pure(ma, b)

    def map[B: ClassTag](f: A => B): M[B] = self.map(ma, f)

    def reduce(f: (A, A) => A): M[A] = self.reduce(ma, f)

    def cross[B: ClassTag](mb: M[B]): M[(A, B)] = self.cross(ma, mb)
  }

  object Ops {
    implicit def mkMOps[A](xs: M[A]): MOps[A] = new MOps[A](xs)
  }
}

object CollectionType {
  implicit def scalaCollectionType[M[_] <: Traversable[_]](
    implicit cbf: CanBuildFrom[M[_], _, M[_]]): CollectionType[M] =
    new CollectionType[M] {
      override def map[A, B: ClassTag](ma: M[A], f: A => B): M[B] = {
        val builder = cbf().asInstanceOf[mutable.Builder[B, M[B]]]
        ma.asInstanceOf[Seq[A]].foreach(a => builder += f(a))
        builder.result()
      }

      override def pure[A: ClassTag](ma: M[_], a: A): M[A] = {
        val builder = cbf().asInstanceOf[mutable.Builder[A, M[A]]]
        builder += a
        builder.result()
      }

      override def reduce[A](ma: M[A], f: (A, A) => A): M[A] = {
        val builder = cbf().asInstanceOf[mutable.Builder[A, M[A]]]
        builder += ma.asInstanceOf[Seq[A]].reduce(f)
        builder.result()
      }

      override def cross[A, B: ClassTag](ma: M[A], mb: M[B]): M[(A, B)] = {
        val builder = cbf().asInstanceOf[mutable.Builder[(A, B), M[(A, B)]]]
        val b = mb.asInstanceOf[Seq[B]].head
        ma.asInstanceOf[Seq[A]].foreach(a => builder += ((a, b)))
        builder.result()
      }
    }

  implicit val arrayCollectionType: CollectionType[Array] = new CollectionType[Array] {
    override def map[A, B: ClassTag](ma: Array[A], f: A => B): Array[B] =
      ma.map(f)

    override def pure[A: ClassTag](ma: Array[_], a: A): Array[A] = Array(a)

    override def reduce[A](ma: Array[A], f: (A, A) => A): Array[A] = {
      // workaround for "No ClassTag available for A"
      val r = ma.take(1)
      r(0) = ma.reduce(f)
      r
    }
    override def cross[A, B: ClassTag](ma: Array[A], mb: Array[B]): Array[(A, B)] =
      ma.map((_, mb.head))
  }
}
