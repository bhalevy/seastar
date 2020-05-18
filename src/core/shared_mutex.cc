/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright (C) 2020 ScyllaDB
 */

#include <seastar/core/future.hh>
#include <seastar/core/shared_mutex.hh>

namespace seastar {

future<> shared_mutex::wait(shared_mutex::is_for_write for_write) noexcept {
    try {
        _waiters.emplace_back(promise<>(), for_write);
    } catch (...) {
        return current_exception_as_future();
    }
    return _waiters.back().pr.get_future();
}

} // namespace seastar
