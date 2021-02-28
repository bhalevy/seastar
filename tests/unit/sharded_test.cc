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
 *  Copyright (C) 2018 Scylladb, Ltd.
 */

#include <seastar/testing/thread_test_case.hh>

#include <seastar/core/sharded.hh>

using namespace seastar;

namespace {
class invoke_on_during_stop final : public peering_sharded_service<invoke_on_during_stop> {
    bool flag = false;

public:
    future<> stop() {
        return container().invoke_on(0, [] (invoke_on_during_stop& instance) {
            instance.flag = true;
        });
    }

    ~invoke_on_during_stop() {
        if (this_shard_id() == 0) {
            assert(flag);
        }
    }
};
}

SEASTAR_THREAD_TEST_CASE(invoke_on_during_stop_test) {
    sharded<invoke_on_during_stop> s;
    auto s_controller = sharded_controller(s);
}

class mydata {
public:
    int x;
    mydata(int value = 1) noexcept : x(value) {}
    future<> stop() {
        return make_ready_future<>();
    }
};

SEASTAR_THREAD_TEST_CASE(invoke_map_returns_non_future_value) {
    seastar::sharded<mydata> s;
    auto s_controller = sharded_controller(s);
    s.map([] (mydata& m) {
        return m.x;
    }).then([] (std::vector<int> results) {
        for (auto& x : results) {
            assert(x == 1);
        }
    }).get();
};

SEASTAR_THREAD_TEST_CASE(invoke_map_returns_future_value) {
    seastar::sharded<mydata> s;
    auto s_controller = sharded_controller(s);
    s.map([] (mydata& m) {
        return make_ready_future<int>(m.x);
    }).then([] (std::vector<int> results) {
        for (auto& x : results) {
            assert(x == 1);
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(invoke_map_returns_future_value_from_thread) {
    seastar::sharded<mydata> s;
    auto s_controller = sharded_controller(s);
    s.map([] (mydata& m) {
        return seastar::async([&m] {
            return m.x;
        });
    }).then([] (std::vector<int> results) {
        for (auto& x : results) {
            assert(x == 1);
        }
    }).get();
}

SEASTAR_THREAD_TEST_CASE(failed_sharded_start_doesnt_hang) {
    class fail_to_start {
    public:
        fail_to_start() { throw 0; }
    };

    seastar::sharded<fail_to_start> s;
    s.start().then_wrapped([] (auto&& fut) { fut.ignore_ready_future(); }).get();
}

SEASTAR_THREAD_TEST_CASE(failed_sharded_start_in_controller) {
    class expected_exception : public std::runtime_error {
    public:
        expected_exception() : runtime_error("expected") {}
    };
    struct fail_to_start {
        fail_to_start() {
            BOOST_TEST_MESSAGE("fail_to_start");
            throw expected_exception();
        }
    };

    seastar::sharded<fail_to_start> s1;
    BOOST_CHECK_THROW(s1.start().get(), expected_exception);

    seastar::sharded<fail_to_start> s2;
    std::unique_ptr<sharded_controller<fail_to_start>> s2_controller;
    auto make_s2_controller = [&s2, &s2_controller] {
        s2_controller = std::make_unique<sharded_controller<fail_to_start>>(s2);
    };
    BOOST_CHECK_THROW(make_s2_controller(), expected_exception);
}

SEASTAR_THREAD_TEST_CASE(sharded_controller_start_with_value) {
    seastar::sharded<mydata> s;
    auto s_controller = sharded_controller(s, 42);
    s.map([] (mydata& m) {
        return seastar::async([&m] {
            return m.x;
        });
    }).then([] (std::vector<int> results) {
        for (auto& x : results) {
            assert(x == 42);
        }
    }).get();
}
