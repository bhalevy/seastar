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
 * Copyright (C) 2022 ScyllaDB
 */

#include <exception>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <seastar/core/immediate.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/exception.hh>
#include <seastar/coroutine/immediate.hh>
#include <seastar/util/later.hh>

using namespace seastar;

class expected_exception : public std::exception {
public:
    expected_exception() = default;
    const char* what() const noexcept override {
        return "expected_exception";
    }
};

static auto make_expected_exception_ptr() noexcept {
    return std::make_exception_ptr(expected_exception());
}

SEASTAR_THREAD_TEST_CASE(test_void_immediate) {
    auto res = immediate<>();
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(!res.available());
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = make_immediate();
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = immediate<>();
    res.set_value();
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = immediate<>();
    res.set_exception(make_expected_exception_ptr());
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(test_non_void_immediate) {
    auto res = immediate<int>();
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(!res.available());
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = make_immediate<int>(42);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = immediate<int>(42);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = immediate<int>();
    res.set_value(17);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 17);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = immediate<int>();
    res.set_exception(make_expected_exception_ptr());
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(test_void_immediate_invoke) {
    bool called = false;
    auto res = seastar::immediate_invoke([&] { called = true; });
    BOOST_REQUIRE(called);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());

    res = seastar::immediate_invoke([] { throw expected_exception(); });
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_THREAD_TEST_CASE(test_non_void_immediate_invoke) {
    auto res = seastar::immediate_invoke([&] () { return 42; });
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    res = seastar::immediate_invoke([] () -> int { throw expected_exception(); });
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_THREAD_TEST_CASE(test_void_nested_immediate_invoke) {
    auto res = seastar::immediate_invoke([&] () { return make_immediate<>(); });
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());

    res = seastar::immediate_invoke([] () { return immediate<>(expected_exception()); });
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_THREAD_TEST_CASE(test_non_void_nested_immediate_invoke) {
    auto res = seastar::immediate_invoke([&] () { return immediate<int>(42); });
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    res = seastar::immediate_invoke([] () { return immediate<int>(expected_exception()); });
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_THREAD_TEST_CASE(test_void_future_immediate) {
    auto res = immediate(make_ready_future<>());
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());

    res = make_future_immediate(make_ready_future<>()).get0();
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());

    res = immediate(make_exception_future<>(expected_exception()));
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);

    res = make_future_immediate(make_exception_future<>(expected_exception())).get0();
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_THREAD_TEST_CASE(test_non_void_future_immediate) {
    auto res = immediate(make_ready_future<int>(42));
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    res = make_future_immediate(make_ready_future<int>(42)).get0();
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    res = immediate(make_exception_future<int>(expected_exception()));
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);

    res = make_future_immediate(make_exception_future<int>(expected_exception())).get0();
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

#ifdef SEASTAR_COROUTINES_ENABLED

SEASTAR_TEST_CASE(test_coroutine_immediate_invoke) {
    bool called = false;
    auto foo = [&] () -> future<immediate<int>> {
        called = true;
        co_return seastar::immediate<int>(42);
    };
    auto res = co_await foo();
    BOOST_REQUIRE(called);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    auto bar = [&] () -> future<immediate<int>> {
        co_return seastar::immediate<int>(make_expected_exception_ptr());
    };
    res = co_await bar();
    BOOST_REQUIRE(called);
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_TEST_CASE(test_coroutine_future_immediate) {
    bool called = false;
    auto foo = [&] () {
        called = true;
        return make_ready_future<int>(42);
    };
    auto res = co_await make_future_immediate(foo());
    BOOST_REQUIRE(called);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    auto bar = [&] () {
        return make_exception_future<int>(expected_exception());
    };
    res = co_await make_future_immediate(bar());
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_TEST_CASE(test_void_coroutine_immediate) {
    auto res = co_await coroutine::immediate(make_ready_future<>());
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());

    res = co_await coroutine::immediate(yield().then([] { return make_ready_future<>(); }));
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());

    res = co_await coroutine::immediate(make_exception_future<>(expected_exception()));
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);

    res = co_await coroutine::immediate(yield().then([] { return make_exception_future<>(expected_exception()); }));
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_TEST_CASE(test_non_void_coroutine_immediate) {
    auto res = co_await coroutine::immediate(make_ready_future<int>(42));
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    res = co_await coroutine::immediate(yield().then([] { return make_ready_future<int>(42); }));
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    res = co_await coroutine::immediate(make_exception_future<int>(expected_exception()));
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);

    res = co_await coroutine::immediate(yield().then([] { return make_exception_future<int>(expected_exception()); }));
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

#endif // SEASTAR_COROUTINES_ENABLED
