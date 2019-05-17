/*
 * Copyright 2019 ABSA Group Limited
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

package za.co.absa.spline.fixture.spline

import org.apache.commons.lang3.NotImplementedException
import za.co.absa.spline.harvester.LineageDispatcher
import za.co.absa.spline.model.DataLineage
import za.co.absa.spline.model.streaming.ProgressEvent


class LineageCapturingDispatcher(lineageCaptor: LineageCaptor.Setter) extends LineageDispatcher {

  override def send(dataLineage: DataLineage): Unit =
    lineageCaptor.capture(dataLineage)

  override def send(event: ProgressEvent): Unit =
    throw new NotImplementedException("Capturing progress events is not supported by this factory yet")
}
