/*
 * Copyright 2020 ABSA Group Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package za.co.absa.spline.persistence

import java.time.{Clock, Instant, ZoneId, ZonedDateTime}

import com.arangodb.async.ArangoDatabaseAsync
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito.{times, verify, when}
import org.scalatest.concurrent.ScalaFutures.whenReady
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.{Assertions, OneInstancePerTest}
import org.scalatestplus.mockito.MockitoSugar
import org.scalatestplus.mockito.MockitoSugar.mock
import za.co.absa.commons.version.impl.SemVer20Impl.SemanticVersion
import za.co.absa.spline.persistence.ArangoManagerImplSpec._
import za.co.absa.spline.persistence.migration.Migrator

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class ArangoManagerImplSpec
  extends AsyncFlatSpec
    with OneInstancePerTest
    with MockitoSugar
    with Matchers {

  private val drmMock = mock[DataRetentionManager]
  when(drmMock.pruneBefore(any())).thenReturn(Future.successful())

  it should "compute timestamp of cut correctly" in {
    val clock = Clock.fixed(Instant.ofEpochMilli(1000000), ZoneId.of("UTC"))
    val manager = newManager(drmMock, clock)

    whenReady(manager.prune(0.millis))(_ => verify(drmMock).pruneBefore(1000000))
    whenReady(manager.prune(1.millis))(_ => verify(drmMock).pruneBefore(999999))
    whenReady(manager.prune(1.second))(_ => verify(drmMock).pruneBefore(999000))
    whenReady(manager.prune(1.minute))(_ => verify(drmMock).pruneBefore(940000))

    Assertions.succeed
  }

  it should "not depend on timezone" in {
    val clock = Clock.fixed(Instant.ofEpochMilli(1000000), ZoneId.of("UTC"))
    val tzPrague = ZoneId.of("Europe/Prague")
    val tzSamara = ZoneId.of("Europe/Samara")

    for {
      _ <- newManager(drmMock, clock.withZone(tzPrague)).prune(1.milli)
      _ <- newManager(drmMock, clock.withZone(tzSamara)).prune(1.milli)
    } yield {
      verify(drmMock, times(2)).pruneBefore(999999)
      Assertions.succeed
    }
  }

  behavior of "prune(ZonedDateTime)"

  it should "compute timestamp of cut correctly" in {
    val clock = Clock.fixed(Instant.ofEpochMilli(1000000), ZoneId.of("UTC"))
    val manager = newManager(drmMock, clock)

    whenReady(manager.prune(ZonedDateTime.now(clock)))(_ => verify(drmMock).pruneBefore(1000000))

    Assertions.succeed
  }

}

object ArangoManagerImplSpec {
  private def newManager(drmMock: DataRetentionManager, clock: Clock)(implicit ec: ExecutionContext): ArangoManagerImpl = {
    new ArangoManagerImpl(
      mock[ArangoDatabaseAsync],
      mock[DatabaseVersionManager],
      drmMock,
      mock[Migrator],
      clock,
      mock[SemanticVersion])
  }
}
