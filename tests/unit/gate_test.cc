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
 * Copyright 2021 ScyllaDB
 */

#include <exception>
#include <functional>

#include <seastar/core/coroutine.hh>
#include <seastar/core/gate.hh>
#include <seastar/core/format.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

using namespace seastar;

// FIXME: name is not used yet. prepared for named gate
template <typename Func>
static future<> check_gate_closed_exception(Func func, sstring name = "") {
    try {
        co_await futurize_invoke(func);
        BOOST_FAIL("func was expected to throw gate_closed_exception");
    } catch (const gate_closed_exception& e) {
        sstring what = e.what();
        BOOST_REQUIRE(what.find("gate closed") != sstring::npos);
        if (!name.empty()) {
            BOOST_REQUIRE(what.find(name) != sstring::npos);
        }
    } catch (...) {
        BOOST_FAIL(format("unexpected exception: {}", std::current_exception()));
    }
}

SEASTAR_TEST_CASE(basic_gate_test) {
    gate g;

    BOOST_REQUIRE_EQUAL(g.get_count(), 0);
    BOOST_REQUIRE(!g.is_closed());
    BOOST_REQUIRE_NO_THROW(g.check());
    BOOST_REQUIRE_EQUAL(g.get_count(), 0);
    BOOST_REQUIRE_NO_THROW(g.enter());
    BOOST_REQUIRE_EQUAL(g.get_count(), 1);
    auto gh0 = g.try_hold();
    BOOST_REQUIRE(gh0.has_value());
    BOOST_REQUIRE_EQUAL(g.get_count(), 2);
    auto gh1 = g.hold();
    BOOST_REQUIRE_EQUAL(g.get_count(), 3);
    BOOST_REQUIRE(!g.is_closed());
    auto f = g.close();
    BOOST_REQUIRE(!f.available());
    g.leave();
    BOOST_REQUIRE_EQUAL(g.get_count(), 2);
    gh0->release();
    BOOST_REQUIRE_EQUAL(g.get_count(), 1);
    gh1.release();
    BOOST_REQUIRE_EQUAL(g.get_count(), 0);
    BOOST_REQUIRE_NO_THROW(co_await std::move(f));
    BOOST_REQUIRE(g.is_closed());
}

SEASTAR_TEST_CASE(gate_closed_test) {
    gate g;

    BOOST_REQUIRE(!g.is_closed());
    BOOST_REQUIRE_NO_THROW(g.check());
    BOOST_REQUIRE_NO_THROW(g.enter());
    auto gh0 = g.try_hold();
    BOOST_REQUIRE(gh0.has_value());
    auto gh1 = g.hold();
    BOOST_REQUIRE(!g.is_closed());
    auto f = g.close();
    BOOST_REQUIRE(!f.available());
    g.leave();
    gh0->release();
    gh1.release();
    BOOST_REQUIRE_NO_THROW(co_await std::move(f));
    BOOST_REQUIRE(g.is_closed());

    BOOST_REQUIRE(!g.try_hold().has_value());

    co_await check_gate_closed_exception([&] { g.check(); });
    co_await check_gate_closed_exception([&] { g.enter(); });
    co_await check_gate_closed_exception([&] { g.hold(); });
    co_await check_gate_closed_exception([&] () -> future<> { co_await with_gate(g, [] { return make_ready_future(); }); });
    co_await check_gate_closed_exception([&] () -> future<> { co_await try_with_gate(g, [] { return make_ready_future(); }); });
}

SEASTAR_TEST_CASE(named_gate_closed_test) {
    sstring name = "foo";
    gate g(name);

    BOOST_REQUIRE(!g.is_closed());
    BOOST_REQUIRE_NO_THROW(g.check());
    BOOST_REQUIRE_NO_THROW(g.enter());
    auto gh0 = g.try_hold();
    BOOST_REQUIRE(gh0.has_value());
    auto gh1 = g.hold();
    BOOST_REQUIRE(!g.is_closed());
    auto f = g.close();
    BOOST_REQUIRE(!f.available());
    g.leave();
    gh0->release();
    gh1.release();
    BOOST_REQUIRE_NO_THROW(co_await std::move(f));
    BOOST_REQUIRE(g.is_closed());

    BOOST_REQUIRE(!g.try_hold().has_value());

    co_await check_gate_closed_exception([&] { g.check(); }, name);
    co_await check_gate_closed_exception([&] { g.enter(); }, name);
    co_await check_gate_closed_exception([&] { g.hold(); }, name);
    co_await check_gate_closed_exception([&] () -> future<> { co_await with_gate(g, [] { return make_ready_future(); }); }, name);
    co_await check_gate_closed_exception([&] () -> future<> { co_await try_with_gate(g, [] { return make_ready_future(); }); }, name);
}

SEASTAR_TEST_CASE(named_gate_properties) {
    gate unnamed;
    BOOST_REQUIRE_EQUAL(unnamed.name(), "");
    sstring foo = "foo";
    auto g0 = gate(foo);
    BOOST_REQUIRE_EQUAL(g0.name(), foo);
    auto g1 = std::move(g0);
    BOOST_REQUIRE_EQUAL(g1.name(), foo);
    sstring bar = "bar";
    g0 = gate(bar);
    BOOST_REQUIRE_EQUAL(g0.name(), bar);
    g1 = std::move(g0);
    BOOST_REQUIRE_EQUAL(g1.name(), bar);
    return make_ready_future();
}
