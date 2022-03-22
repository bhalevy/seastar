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
 * Copyright (C) 2018 ScyllaDB Ltd.
 */

#include <boost/range.hpp>
#include <boost/range/irange.hpp>

#include <seastar/testing/perf_tests.hh>
#include <seastar/core/loop.hh>
#include <seastar/util/later.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>

struct parallel_for_each {
    std::vector<int> empty_range;
    std::vector<int> range;
    int value;

    static constexpr int range_count = 100;

    parallel_for_each()
        : empty_range()
        , range(boost::copy_range<std::vector<int>>(boost::irange(1, range_count)))
    { }
};

PERF_TEST_F(parallel_for_each, empty)
{
    return seastar::parallel_for_each(empty_range, [] (int) -> future<> {
        abort();
    });
}

[[gnu::noinline]]
future<> immediate(int v, int& vs)
{
    vs += v;
    return make_ready_future<>();
}

PERF_TEST_F(parallel_for_each, immediate, parallel_for_each::range_count)
{
    return seastar::parallel_for_each(range, [this] (int v) {
        return immediate(v, value);
    }).then([this] {
        perf_tests::do_not_optimize(value);
    });
}

[[gnu::noinline]]
future<> suspend(int v, int& vs)
{
    vs += v;
    return yield();
}

PERF_TEST_F(parallel_for_each, suspend, parallel_for_each::range_count)
{
    return seastar::parallel_for_each(range, [this] (int v) {
        return suspend(v, value);
    }).then([this] {
        perf_tests::do_not_optimize(value);
    });
}

#ifdef SEASTAR_COROUTINES_ENABLED

PERF_TEST_C(parallel_for_each, cor_empty)
{
    co_await make_ready_future<>();
    co_await seastar::parallel_for_each(empty_range, [] (int) -> future<> {
        abort();
    });
}

PERF_TEST_C(parallel_for_each, cor_immediate, parallel_for_each::range_count)
{
    co_await seastar::parallel_for_each(range, [this] (int v) {
        return immediate(v, value);
    });
    perf_tests::do_not_optimize(value);
}

PERF_TEST_C(parallel_for_each, cor_suspend, parallel_for_each::range_count)
{
    co_await seastar::parallel_for_each(range, [this] (int v) {
        return suspend(v, value);
    });
    perf_tests::do_not_optimize(value);
}

PERF_TEST_C(parallel_for_each, cor_pfe_empty)
{
    co_await seastar::coroutine::parallel_for_each(empty_range, [] (int) -> future<> {
        abort();
    });
}

PERF_TEST_C(parallel_for_each, cor_pfe_immediate, parallel_for_each::range_count)
{
    co_await seastar::coroutine::parallel_for_each(range, [this] (int v) {
        return immediate(v, value);
    });
    perf_tests::do_not_optimize(value);
}

PERF_TEST_C(parallel_for_each, cor_pfe_suspend, parallel_for_each::range_count)
{
    co_await seastar::coroutine::parallel_for_each(range, [this] (int v) {
        return suspend(v, value);
    });
    perf_tests::do_not_optimize(value);
}

#endif // SEASTAR_COROUTINES_ENABLED