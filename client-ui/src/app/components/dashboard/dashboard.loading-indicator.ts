/*
 * Copyright 2019 ABSA Group Limited
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

const LOADING_INDICATOR_DELAY = 300 //millis

export class DashboardLoadingIndicator {
  private loading: boolean
  private timeoutId: number

  public get isActive(): boolean {
    return this.loading
  }

  public activate(): void {
    this.timeoutId = setTimeout(
      () => this.loading = true,
      LOADING_INDICATOR_DELAY)
  }

  public deactivate(): void {
    this.loading = false
    clearTimeout(this.timeoutId)
  }
}
