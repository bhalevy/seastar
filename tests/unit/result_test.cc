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

#include <seastar/core/result.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/exception.hh>

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

SEASTAR_THREAD_TEST_CASE(test_void_result) {
    auto res = result<>();
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(!res.available());
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = result<>();
    res.set_value();
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = result<>();
    res.set_exception(make_expected_exception_ptr());
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(test_void_invoke_result) {
    bool called = false;
    auto res = seastar::invoke_result([&] { called = true; });
    BOOST_REQUIRE(called);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_NO_THROW(res.get());

    res = seastar::invoke_result([] { throw expected_exception(); });
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

SEASTAR_THREAD_TEST_CASE(test_non_void_result) {
    auto res = result<int>();
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(!res.available());
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = result<int>(42);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = result<int>();
    res.set_value(17);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 17);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);

    res = result<int>();
    res.set_exception(make_expected_exception_ptr());
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
    BOOST_REQUIRE_THROW(res.get(), std::runtime_error);
}

SEASTAR_THREAD_TEST_CASE(test_non_void_invoke_result) {
    bool called = false;
    auto res = seastar::invoke_result([&] () { called = true; return 42; });
    BOOST_REQUIRE(called);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    res = seastar::invoke_result([] () -> int { throw expected_exception(); });
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

#ifdef SEASTAR_COROUTINES_ENABLED

SEASTAR_TEST_CASE(test_coroutine_invoke_result) {
    bool called = false;
    auto foo = [&] () -> future<result<int>> {
        called = true;
        co_return seastar::result<int>(42);
    };
    auto res = co_await foo();
    BOOST_REQUIRE(called);
    BOOST_REQUIRE(res.has_value());
    BOOST_REQUIRE(!res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_EQUAL(res.get(), 42);

    auto bar = [&] () -> future<result<int>> {
        co_return seastar::result<int>(make_expected_exception_ptr());
    };
    res = co_await bar();
    BOOST_REQUIRE(called);
    BOOST_REQUIRE(!res.has_value());
    BOOST_REQUIRE(res.failed());
    BOOST_REQUIRE(res.available());
    BOOST_REQUIRE_THROW(res.get(), expected_exception);
}

#endif // SEASTAR_COROUTINES_ENABLED
