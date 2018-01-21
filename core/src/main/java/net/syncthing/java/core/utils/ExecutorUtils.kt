/* 
 * Copyright (C) 2016 Davide Imbriaco
 *
 * This Java file is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.syncthing.java.core.utils

import org.slf4j.LoggerFactory
import java.util.concurrent.ExecutorService
import java.util.concurrent.TimeUnit

object ExecutorUtils {

    private val logger = LoggerFactory.getLogger(ExecutorUtils::class.java)

    fun awaitTerminationSafe(executorService: ExecutorService) {
        try {
            executorService.awaitTermination(2, TimeUnit.SECONDS)
        } catch (ex: InterruptedException) {
            logger.warn("", ex)
        }

    }
}
