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

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/tmp_file.hh>

using namespace seastar;
namespace fs = compat::filesystem;

SEASTAR_TEST_CASE(test_make_tmp_file) {
    return make_tmp_file().then([] (tmp_file tf) {
        return async([tf = std::move(tf)] () mutable {
            const sstring tmp_path = tf.get_path().native();
            BOOST_REQUIRE(file_exists(tmp_path).get0());
            tf.close().get();
            tf.remove().get();
            BOOST_REQUIRE(!file_exists(tmp_path).get0());
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_tmp_file) {
    size_t expected = ~0;
    size_t actual = 0;

    tmp_file::do_with([&] (tmp_file& tf) mutable {
        auto& f = tf.get_file();
        auto buf = temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment());
        return do_with(std::move(buf), [&] (auto& buf) mutable {
            expected = buf.size();
            return f.dma_write(0, buf.get(), buf.size()).then([&] (size_t written) {
                actual = written;
                return make_ready_future<>();
            });
        });
    }).get();
    BOOST_REQUIRE_EQUAL(expected , actual);
}

SEASTAR_THREAD_TEST_CASE(test_tmp_file_with_path) {
    size_t expected = ~0;
    size_t actual = 0;

    tmp_file::do_with("/tmp", [&] (tmp_file& tf) mutable {
        auto& f = tf.get_file();
        auto buf = temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment());
        return do_with(std::move(buf), [&] (auto& buf) mutable {
            expected = buf.size();
            return f.dma_write(0, buf.get(), buf.size()).then([&] (size_t written) {
                actual = written;
                return make_ready_future<>();
            });
        });
    }).get();
    BOOST_REQUIRE_EQUAL(expected , actual);
}

SEASTAR_TEST_CASE(test_make_tmp_dir) {
    return make_tmp_dir().then([] (tmp_dir td) {
        return async([td = std::move(td)] () mutable {
            const sstring tmp_path = td.get_path().native();
            BOOST_REQUIRE(file_exists(tmp_path).get0());
            td.remove().get();
            BOOST_REQUIRE(!file_exists(tmp_path).get0());
        });
    });
}

SEASTAR_THREAD_TEST_CASE(test_tmp_dir) {
    size_t expected;
    size_t actual;
    tmp_dir::do_with([&] (tmp_dir& td) {
        return tmp_file::do_with(td.get_path(), [&] (tmp_file& tf) {
            auto& f = tf.get_file();
            auto buf = temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment());
            return do_with(std::move(buf), [&] (auto& buf) mutable {
                expected = buf.size();
                return f.dma_write(0, buf.get(), buf.size()).then([&] (size_t written) {
                    actual = written;
                    return make_ready_future<>();
                });
            });
        });
    }).get();
    BOOST_REQUIRE_EQUAL(expected , actual);
}

SEASTAR_THREAD_TEST_CASE(test_tmp_dir_with_path) {
    size_t expected;
    size_t actual;
    tmp_dir::do_with(".", [&] (tmp_dir& td) {
        return tmp_file::do_with(td.get_path(), [&] (tmp_file& tf) {
            auto& f = tf.get_file();
            auto buf = temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment());
            return do_with(std::move(buf), [&] (auto& buf) mutable {
                expected = buf.size();
                return tf.get_file().dma_write(0, buf.get(), buf.size()).then([&] (size_t written) {
                    actual = written;
                    return make_ready_future<>();
                });
            });
        });
    }).get();
    BOOST_REQUIRE_EQUAL(expected , actual);
}

SEASTAR_THREAD_TEST_CASE(test_tmp_dir_with_non_existing_path) {
    BOOST_REQUIRE_EXCEPTION(tmp_dir::do_with("/tmp/this_name_should_not_exist", [] (tmp_dir&) {}).get(),
            std::system_error, testing::exception_predicate::message_contains("No such file or directory"));
}
