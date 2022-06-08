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
 * Copyright (C) 2021-present ScyllaDB
 */

#pragma once

#include <seastar/core/coroutine.hh>
#include <seastar/core/future.hh>

namespace seastar {

namespace internal {

struct exception_awaiter {
    std::exception_ptr eptr;

    explicit exception_awaiter(std::exception_ptr&& eptr) noexcept : eptr(std::move(eptr)) {}

    exception_awaiter(const exception_awaiter&) = delete;
    exception_awaiter(exception_awaiter&&) = delete;

    bool await_ready() const noexcept {
        return false;
    }

    template<typename U>
    void await_suspend(SEASTAR_INTERNAL_COROUTINE_NAMESPACE::coroutine_handle<U> hndl) noexcept {
        hndl.promise().set_exception(std::move(eptr));
        hndl.destroy();
    }

    void await_resume() noexcept {}
};

} // internal

namespace coroutine {

/// Wrapper for propagating an exception directly rather than
/// throwing it. The wrapper can be used with both co_await and co_return.
///
/// \note It is not possible to co_return the wrapper in coroutines which
/// return future<> due to language limitations (it's not possible to specify
/// both return_value and return_void in the promise_type). You can use co_await
/// instead which works in coroutines which return either future<> or future<T>.
///
/// Example usage:
///
/// ```
/// co_await coroutine::exception(std::make_exception_ptr(std::runtime_error("something failed miserably")));
/// co_return coroutine::exception(std::make_exception_ptr(std::runtime_error("something failed miserably")));
/// ```
struct exception {
    std::exception_ptr eptr;

    explicit exception(std::exception_ptr&& eptr) noexcept : eptr(std::move(eptr)) {}
    explicit exception(const std::exception_ptr& eptr) noexcept : eptr(eptr) {}

    template <typename E>
    requires (!std::same_as<std::remove_reference_t<E>, std::exception_ptr>)
    explicit exception(E&& e) noexcept : eptr(std::make_exception_ptr(std::forward<E>(e))) {
        log_exception_trace();
    }
};

/// Allows propagating an exception from a coroutine directly rather than
/// throwing it.
///
/// `make_exception()` returns an object which must be co_returned.
/// Co_returning the object will immediately resolve the current coroutine
/// to the given exception.
///
/// \note Due to language limitations, this function doesn't work in coroutines
/// which return future<>. Consider using return_exception instead.
///
/// Example usage:
///
/// ```
/// co_return coroutine::make_exception(std::runtime_error("something failed miserably"));
/// ```
[[nodiscard]]
exception make_exception(auto&& e) noexcept {
    return exception(std::forward<decltype(e)>(e));
}

/// Allows propagating an exception from a coroutine directly rather than
/// throwing it.
///
/// `return_exception()` returns an object which must be co_awaited.
/// Co_awaiting the object will immediately resolve the current coroutine
/// to the given exception.
///
/// Example usage:
///
/// ```
/// co_await coroutine::return_exception(std::runtime_error("something failed miserably"));
/// ```
[[nodiscard]]
exception return_exception(auto&& e) noexcept {
    return exception(std::forward<decltype(e)>(e));
}

} // coroutine

inline auto operator co_await(coroutine::exception ex) noexcept {
    return internal::exception_awaiter(std::move(ex.eptr));
}

} // seastar        auto ex = ;

