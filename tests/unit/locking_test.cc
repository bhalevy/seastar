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
 * Copyright (C) 2020 ScyllaDB.
 */

#include <chrono>

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/core/do_with.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/rwlock.hh>
#include <boost/range/irange.hpp>

using namespace seastar;
using namespace std::chrono_literals;

SEASTAR_THREAD_TEST_CASE(test_rwlock) {
    rwlock l;

    l.for_write().lock().get();
    BOOST_REQUIRE(!l.for_write().try_lock());
    BOOST_REQUIRE(!l.for_read().try_lock());
    l.for_write().unlock();

    l.for_read().lock().get();
    BOOST_REQUIRE(!l.for_write().try_lock());
    BOOST_REQUIRE(l.for_read().try_lock());
    l.for_read().lock().get();
    l.for_read().unlock();
    l.for_read().unlock();
    l.for_read().unlock();

    BOOST_REQUIRE(l.for_write().try_lock());
    l.for_write().unlock();
}

SEASTAR_TEST_CASE(test_rwlock_exclusive) {
    return do_with(rwlock(), unsigned(0), [] (rwlock& l, unsigned& counter) {
        return parallel_for_each(boost::irange(0, 10), [&l, &counter] (int idx) {
            return with_lock(l.for_write(), [&counter] {
                BOOST_REQUIRE_EQUAL(counter, 0u);
                ++counter;
                return sleep(1ms).then([&counter] {
                    --counter;
                    BOOST_REQUIRE_EQUAL(counter, 0u);
                });
            });
        });
    });
}

SEASTAR_TEST_CASE(test_rwlock_shared) {
    return do_with(rwlock(), unsigned(0), unsigned(0), [] (rwlock& l, unsigned& counter, unsigned& max) {
        return parallel_for_each(boost::irange(0, 10), [&l, &counter, &max] (int idx) {
            return with_lock(l.for_read(), [&counter, &max] {
                ++counter;
                max = std::max(max, counter);
                return sleep(1ms).then([&counter] {
                    --counter;
                });
            });
        }).finally([&counter, &max] {
            BOOST_REQUIRE_EQUAL(counter, 0u);
            BOOST_REQUIRE_NE(max, 0u);
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_rwlock_failed_func) {
    rwlock l;

    // verify that the rwlock is unlocked when func fails
    future<> fut = with_lock(l.for_read(), [] {
        throw std::runtime_error("injected");
    });
    BOOST_REQUIRE_THROW(fut.get(), std::runtime_error);

    fut = with_lock(l.for_write(), [] {
        throw std::runtime_error("injected");
    });
    BOOST_REQUIRE_THROW(fut.get(), std::runtime_error);

    BOOST_REQUIRE(l.for_write().try_lock());
    l.for_write().unlock();
}
