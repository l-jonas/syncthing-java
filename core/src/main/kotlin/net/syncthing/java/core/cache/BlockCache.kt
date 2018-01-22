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
package net.syncthing.java.core.cache

import net.syncthing.java.core.configuration.ConfigurationService

abstract class BlockCache {

    /**
     * note: The data may be written to disk in background. Do not modify the
     * supplied array
     *
     * @param data
     * @return cache block code, or null in case of errors
     */
    abstract fun pushBlock(data: ByteArray): String?

    abstract fun pushData(code: String, data: ByteArray): Boolean

    abstract fun pullBlock(code: String): ByteArray?

    abstract fun pullData(code: String): ByteArray?

    open fun clear() {}

    private class DummyBlockCache : BlockCache() {

        override fun pushBlock(data: ByteArray): String? {
            return null
        }

        override fun pullBlock(code: String): ByteArray? {
            return null
        }

        override fun pushData(code: String, data: ByteArray): Boolean {
            return false
        }

        override fun pullData(code: String): ByteArray? {
            return null
        }

    }

    companion object {

        fun getBlockCache(configuration: ConfigurationService): BlockCache {
            return FileBlockCache(configuration.cache)
        }
    }

}
