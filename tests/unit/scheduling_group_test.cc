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
 * Copyright (C) 2017 ScyllaDB Ltd.
 */

#include "seastar/core/preempt.hh"
#include <algorithm>
#include <vector>
#include <chrono>

#include <seastar/core/thread.hh>
#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>
#include <seastar/testing/test_runner.hh>
#include <seastar/core/execution_stage.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/print.hh>
#include <seastar/core/scheduling_specific.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/with_scheduling_group.hh>
#include <seastar/util/later.hh>
#include <seastar/core/when_all.hh>
#include <seastar/core/reactor.hh>

using namespace std::chrono_literals;

using namespace seastar;

/**
 *  Test setting primitive and object as a value after all groups are created
 */
SEASTAR_THREAD_TEST_CASE(sg_specific_values_define_after_sg_create) {
    using ivec  = std::vector<int>;
    const int num_scheduling_groups = 4;
    std::vector<scheduling_group> sgs;
    for (int i = 0; i < num_scheduling_groups; i++) {
        sgs.push_back(create_scheduling_group(format("sg{}", i).c_str(), 100).get0());
    }

    const auto destroy_scheduling_groups = defer([&sgs] () {
       for (scheduling_group sg : sgs) {
           destroy_scheduling_group(sg).get();
       }
    });
    scheduling_group_key_config key1_conf = make_scheduling_group_key_config<int>();
    scheduling_group_key key1 = scheduling_group_key_create(key1_conf).get0();

    scheduling_group_key_config key2_conf = make_scheduling_group_key_config<ivec>();
    scheduling_group_key key2 = scheduling_group_key_create(key2_conf).get0();

    smp::invoke_on_all([key1, key2, &sgs] () {
        int factor = this_shard_id() + 1;
        for (int i=0; i < num_scheduling_groups; i++) {
            sgs[i].get_specific<int>(key1) = (i + 1) * factor;
            sgs[i].get_specific<ivec>(key2).push_back((i + 1) * factor);
        }

        for (int i=0; i < num_scheduling_groups; i++) {
            BOOST_REQUIRE_EQUAL(sgs[i].get_specific<int>(key1) = (i + 1) * factor, (i + 1) * factor);
            BOOST_REQUIRE_EQUAL(sgs[i].get_specific<ivec>(key2)[0], (i + 1) * factor);
        }

    }).get();

    smp::invoke_on_all([key1, key2] () {
        return reduce_scheduling_group_specific<int>(std::plus<int>(), int(0), key1).then([] (int sum) {
            int factor = this_shard_id() + 1;
            int expected_sum = ((1 + num_scheduling_groups)*num_scheduling_groups) * factor /2;
            BOOST_REQUIRE_EQUAL(expected_sum, sum);
        }). then([key2] {
            auto ivec_to_int = [] (ivec& v) {
                return v.size() ? v[0] : 0;
            };

            return map_reduce_scheduling_group_specific<ivec>(ivec_to_int, std::plus<int>(), int(0), key2).then([] (int sum) {
                int factor = this_shard_id() + 1;
                int expected_sum = ((1 + num_scheduling_groups)*num_scheduling_groups) * factor /2;
                BOOST_REQUIRE_EQUAL(expected_sum, sum);
            });

        });
    }).get();


}

/**
 *  Test setting primitive and object as a value before all groups are created
 */
SEASTAR_THREAD_TEST_CASE(sg_specific_values_define_before_sg_create) {
    using ivec  = std::vector<int>;
    const int num_scheduling_groups = 4;
    std::vector<scheduling_group> sgs;
    const auto destroy_scheduling_groups = defer([&sgs] () {
       for (scheduling_group sg : sgs) {
           destroy_scheduling_group(sg).get();
       }
    });
    scheduling_group_key_config key1_conf = make_scheduling_group_key_config<int>();
    scheduling_group_key key1 = scheduling_group_key_create(key1_conf).get0();

    scheduling_group_key_config key2_conf = make_scheduling_group_key_config<ivec>();
    scheduling_group_key key2 = scheduling_group_key_create(key2_conf).get0();

    for (int i = 0; i < num_scheduling_groups; i++) {
        sgs.push_back(create_scheduling_group(format("sg{}", i).c_str(), 100).get0());
    }

    smp::invoke_on_all([key1, key2, &sgs] () {
        int factor = this_shard_id() + 1;
        for (int i=0; i < num_scheduling_groups; i++) {
            sgs[i].get_specific<int>(key1) = (i + 1) * factor;
            sgs[i].get_specific<ivec>(key2).push_back((i + 1) * factor);
        }

        for (int i=0; i < num_scheduling_groups; i++) {
            BOOST_REQUIRE_EQUAL(sgs[i].get_specific<int>(key1) = (i + 1) * factor, (i + 1) * factor);
            BOOST_REQUIRE_EQUAL(sgs[i].get_specific<ivec>(key2)[0], (i + 1) * factor);
        }

    }).get();

    smp::invoke_on_all([key1, key2] () {
        return reduce_scheduling_group_specific<int>(std::plus<int>(), int(0), key1).then([] (int sum) {
            int factor = this_shard_id() + 1;
            int expected_sum = ((1 + num_scheduling_groups)*num_scheduling_groups) * factor /2;
            BOOST_REQUIRE_EQUAL(expected_sum, sum);
        }). then([key2] {
            auto ivec_to_int = [] (ivec& v) {
                return v.size() ? v[0] : 0;
            };

            return map_reduce_scheduling_group_specific<ivec>(ivec_to_int, std::plus<int>(), int(0), key2).then([] (int sum) {
                int factor = this_shard_id() + 1;
                int expected_sum = ((1 + num_scheduling_groups)*num_scheduling_groups) * factor /2;
                BOOST_REQUIRE_EQUAL(expected_sum, sum);
            });

        });
    }).get();

}

/**
 *  Test setting primitive and an object as a value before some groups are created
 *  and after some of the groups are created.
 */
SEASTAR_THREAD_TEST_CASE(sg_specific_values_define_before_and_after_sg_create) {
    using ivec  = std::vector<int>;
    const int num_scheduling_groups = 4;
    std::vector<scheduling_group> sgs;
    const auto destroy_scheduling_groups = defer([&sgs] () {
       for (scheduling_group sg : sgs) {
           destroy_scheduling_group(sg).get();
       }
    });

    for (int i = 0; i < num_scheduling_groups/2; i++) {
        sgs.push_back(create_scheduling_group(format("sg{}", i).c_str(), 100).get0());
    }
    scheduling_group_key_config key1_conf = make_scheduling_group_key_config<int>();
    scheduling_group_key key1 = scheduling_group_key_create(key1_conf).get0();

    scheduling_group_key_config key2_conf = make_scheduling_group_key_config<ivec>();
    scheduling_group_key key2 = scheduling_group_key_create(key2_conf).get0();

    for (int i = num_scheduling_groups/2; i < num_scheduling_groups; i++) {
        sgs.push_back(create_scheduling_group(format("sg{}", i).c_str(), 100).get0());
    }

    smp::invoke_on_all([key1, key2, &sgs] () {
        int factor = this_shard_id() + 1;
        for (int i=0; i < num_scheduling_groups; i++) {
            sgs[i].get_specific<int>(key1) = (i + 1) * factor;
            sgs[i].get_specific<ivec>(key2).push_back((i + 1) * factor);
        }

        for (int i=0; i < num_scheduling_groups; i++) {
            BOOST_REQUIRE_EQUAL(sgs[i].get_specific<int>(key1) = (i + 1) * factor, (i + 1) * factor);
            BOOST_REQUIRE_EQUAL(sgs[i].get_specific<ivec>(key2)[0], (i + 1) * factor);
        }

    }).get();

    smp::invoke_on_all([key1, key2] () {
        return reduce_scheduling_group_specific<int>(std::plus<int>(), int(0), key1).then([] (int sum) {
            int factor = this_shard_id() + 1;
            int expected_sum = ((1 + num_scheduling_groups)*num_scheduling_groups) * factor /2;
            BOOST_REQUIRE_EQUAL(expected_sum, sum);
        }). then([key2] {
            auto ivec_to_int = [] (ivec& v) {
                return v.size() ? v[0] : 0;
            };

            return map_reduce_scheduling_group_specific<ivec>(ivec_to_int, std::plus<int>(), int(0), key2).then([] (int sum) {
                int factor = this_shard_id() + 1;
                int expected_sum = ((1 + num_scheduling_groups)*num_scheduling_groups) * factor /2;
                BOOST_REQUIRE_EQUAL(expected_sum, sum);
            });

        });
    }).get();
}

/*
 * Test that current scheduling group is inherited by seastar::async()
 */
SEASTAR_THREAD_TEST_CASE(sg_scheduling_group_inheritance_in_seastar_async_test) {
    scheduling_group sg = create_scheduling_group("sg0", 100).get0();
    thread_attributes attr = {};
    attr.sched_group = sg;
    seastar::async(attr, [attr] {
        BOOST_REQUIRE_EQUAL(internal::scheduling_group_index(current_scheduling_group()),
                                internal::scheduling_group_index(*(attr.sched_group)));

        seastar::async([attr] {
            BOOST_REQUIRE_EQUAL(internal::scheduling_group_index(current_scheduling_group()),
                                internal::scheduling_group_index(*(attr.sched_group)));

            smp::invoke_on_all([sched_group_idx = internal::scheduling_group_index(*(attr.sched_group))] () {
                BOOST_REQUIRE_EQUAL(internal::scheduling_group_index(current_scheduling_group()), sched_group_idx);
            }).get();
        }).get();
    }).get();
}


SEASTAR_THREAD_TEST_CASE(later_preserves_sg) {
    scheduling_group sg = create_scheduling_group("sg", 100).get0();
    auto cleanup = defer([&] { destroy_scheduling_group(sg).get(); });
    with_scheduling_group(sg, [&] {
        return later().then([&] {
            BOOST_REQUIRE_EQUAL(
                    internal::scheduling_group_index(current_scheduling_group()),
                    internal::scheduling_group_index(sg));
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(simple_sg_scheduling) {
    constexpr std::array<unsigned, 2> shares = { 100, 200 };
    std::array<scheduling_group, 2> sched_groups;
    sched_groups[0] = create_scheduling_group("sg0", shares[0]).get0();
    sched_groups[1] = create_scheduling_group("sg1", shares[1]).get0();
    auto cleanup = defer([&] {
        do_for_each(sched_groups, [] (scheduling_group& sg) {
            return destroy_scheduling_group(sg);
        }).get();
    });
    smp::invoke_on_all([&] {
        return async([&] {
            std::array<uint64_t, 2> counters = { 0, 0 };
            constexpr uint64_t max_count = 1000;
            bool stop = false;
            auto func = [&] (int idx) {
                return with_scheduling_group(sched_groups[idx], [&, idx] {
                    return do_until([&] { return stop || ++counters[idx] >= max_count; }, [&] {
                        while (!need_preempt()) {
                        }
                        return make_ready_future<>();
                    }).then([&] {
                        stop = true;
                    });
                });
            };
            when_all(func(0), func(1)).discard_result().get();
            double ratio = double(counters[1]) / double(counters[0]);
            BOOST_TEST_MESSAGE(format("count[0]={} count[1]={} ratio={:.2f}", counters[0], counters[1], ratio));
            double allowed_deviation = 0.1;
            BOOST_REQUIRE_GT(ratio, (1 - allowed_deviation) * shares[1] / shares[0]);
            BOOST_REQUIRE_LT(ratio, (1 + allowed_deviation) * shares[1] / shares[0]);
        });
    }).get();
}

SEASTAR_THREAD_TEST_CASE(uneven_work_sg_scheduling) {
    constexpr std::array<unsigned, 2> shares = { 100, 200 };
    std::array<scheduling_group, 2> sched_groups;
    sched_groups[0] = create_scheduling_group("sg0", shares[0]).get0();
    sched_groups[1] = create_scheduling_group("sg1", shares[1]).get0();
    auto cleanup = defer([&] {
        do_for_each(sched_groups, [] (scheduling_group& sg) {
            return destroy_scheduling_group(sg);
        }).get();
    });
    while (!need_preempt()) {
    }
    thread::yield();
    int calibrate = 0;
    while (!need_preempt()) {
        calibrate++;
    }
    BOOST_TEST_MESSAGE(format("calibrate={}", calibrate));

    smp::invoke_on_all([&] {
        return async([&] {
            std::array<uint64_t, 2> counters = { 0, 0 };
            std::array<std::vector<int>, 2> vectors;
            auto& eng = testing::local_random_engine;
            auto dist = std::uniform_int_distribution<int>();

            vectors[0].resize(100 + dist(eng) % 200);
            vectors[1].resize(10 + dist(eng) % 20);
            if (dist(eng) & 1) {
                std::swap(vectors[0], vectors[1]);
            }
            for (auto i = 0; i < 2; i++) {
                auto size = vectors[i].size();
                BOOST_TEST_MESSAGE(format("vector[{}] size={}", i, size));
                for (size_t j = 0; j < size; j++) {
                    vectors[i].push_back(dist(eng));
                }
            }

            bool stop = false;
            auto func = [&] (int idx) {
                return with_scheduling_group(sched_groups[idx], [&, idx] {
                  return do_with(int64_t(0), [&, idx] (int64_t& sum) {
                    return do_until([&] { return stop; }, [&, idx] {
                        for (auto it = vectors[idx].cbegin(); it != vectors[idx].cend(); it++) {
                            sum += *it;
                            ++counters[idx];
                        }
                        return make_ready_future<>();
                    }).then([&] {
                        return make_ready_future<int64_t>(sum);
                    });
                  });
                });
            };
            auto f0 = func(0);
            auto f1 = func(1);
            auto stop_fut = sleep(5s).then([&] {
                stop = true;
            });
            when_all(std::move(f0), std::move(f1), std::move(stop_fut)).discard_result().get();
            double ratio = double(counters[1]) / double(counters[0]);
            BOOST_TEST_MESSAGE(format("count[0]={} count[1]={} ratio={:.2f}", counters[0], counters[1], ratio));
            for (auto sg : sched_groups) {
                engine().print_scheduling_group_stats(sg);
            }

            //double shares_ratio = double(shares[1]) / double(shares[0]);
            //double allowed_deviation = 0.1;
            //BOOST_REQUIRE_GT(ratio, (1.0 - allowed_deviation) * shares_ratio);
            //BOOST_REQUIRE_LT(ratio, (1.0 + allowed_deviation) * shares_ratio);
        });
    }).get();
}
