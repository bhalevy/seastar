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
 * Copyright (C) 2022-present ScyllaDB
 */

#pragma once

#include <seastar/core/coroutine.hh>

namespace seastar::coroutine {

/// co_await a \ref seastar::future<T...>, returning the future as result.
///
/// Similar to \ref `seastar::then_wrapped`, `coroutine::as_future`
/// waits for the \ref future to resolve either to a ready future
/// or to an exceptional one. It then returns the future as the co_await result.
///
/// For example:
/// ```
/// static future<bool> did_future_fail(future<> fut) {
///     auto f = co_await coroutine::as_future(std::move(fut));
///     if (f.failed()) {
///         mylog.warn("Future failed: {}", f.get_exception());
///         co_return true;
///     } else {
///         co_return false;
///     }
/// }
/// ```
template <typename... T>
struct as_future {
    seastar::future<T...> _future;
public:
    explicit as_future(seastar::future<T...>&& f) noexcept
        : _future(std::move(f))
    { }

    as_future(const as_future&) = delete;
    as_future(as_future&&) = delete;

    bool await_ready() const noexcept {
        return _future.available();
    }

    template<typename U>
    void await_suspend(SEASTAR_INTERNAL_COROUTINE_NAMESPACE::coroutine_handle<U> hndl) noexcept {
        if (!_future.available()) {
            _future.set_coroutine(hndl.promise());
        } else {
            schedule(&hndl.promise());
        }
    }

    seastar::future<T...> await_resume() {
        return std::move(_future);
    }
};

} // namespace coroutine
