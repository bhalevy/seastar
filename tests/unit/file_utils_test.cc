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
 * Copyright (C) 2014-2015 Cloudius Systems, Ltd.
 */

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <seastar/core/file.hh>
#include <seastar/util/tmp_file.hh>

using namespace seastar;

future<> touch_file(const sstring& filename, open_flags oflags = open_flags::rw | open_flags::create) {
    return open_file_dma(filename, oflags).then([] (file f) {
        return f.close().finally([f] {});
    });
}

SEASTAR_THREAD_TEST_CASE(tmp_name_test) {
    auto filename1 = tmp_name().get0().native();
    BOOST_REQUIRE(!file_exists(filename1).get0());
    BOOST_REQUIRE_NO_THROW(touch_file(filename1, open_flags::rw | open_flags::create | open_flags::exclusive).get());
    BOOST_REQUIRE_EXCEPTION(touch_file(filename1, open_flags::rw | open_flags::create | open_flags::exclusive).get(), std::system_error,
            testing::exception_predicate::message_contains("File exists"));

    auto filename2 = tmp_name().get0().native();
    BOOST_REQUIRE(!file_exists(filename2).get0());
    BOOST_REQUIRE_NO_THROW(touch_file(filename2, open_flags::rw | open_flags::create | open_flags::exclusive).get());
    BOOST_REQUIRE_EXCEPTION(touch_file(filename2, open_flags::rw | open_flags::create | open_flags::exclusive).get(), std::system_error,
            testing::exception_predicate::message_contains("File exists"));

    remove_file(filename1).get();
    remove_file(filename2).get();
}

SEASTAR_THREAD_TEST_CASE(tmp_name_with_path_test) {
    auto filename1 = tmp_name("/tmp").get0().native();
    BOOST_REQUIRE(!file_exists(filename1).get0());
    BOOST_REQUIRE_NO_THROW(touch_file(filename1, open_flags::rw | open_flags::create | open_flags::exclusive).get());
    BOOST_REQUIRE_EXCEPTION(touch_file(filename1, open_flags::rw | open_flags::create | open_flags::exclusive).get(), std::system_error,
            testing::exception_predicate::message_contains("File exists"));

    auto filename2 = tmp_name("/tmp").get0().native();
    BOOST_REQUIRE(!file_exists(filename2).get0());
    BOOST_REQUIRE_NO_THROW(touch_file(filename2, open_flags::rw | open_flags::create | open_flags::exclusive).get());
    BOOST_REQUIRE_EXCEPTION(touch_file(filename2, open_flags::rw | open_flags::create | open_flags::exclusive).get(), std::system_error,
            testing::exception_predicate::message_contains("File exists"));

    remove_file(filename1).get();
    remove_file(filename2).get();
}

SEASTAR_THREAD_TEST_CASE(make_tmp_file_test) {
    auto filename1 = make_tmp_file().get0().native();
    BOOST_REQUIRE(file_exists(filename1).get0());

    auto filename2 = make_tmp_file().get0().native();
    BOOST_REQUIRE(file_exists(filename2).get0());

    remove_file(filename1).get();
    remove_file(filename2).get();
}

SEASTAR_THREAD_TEST_CASE(tmp_file_test) {
    size_t expected = ~0;
    size_t actual = 0;

    tmp_file::do_with([&] (file f) {
        return do_with(temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment()), [&, f] (auto& buf) mutable {
            expected = buf.size();
            return f.dma_write(0, buf.get(), buf.size()).then([&] (size_t written) {
                actual = written;
                return make_ready_future<>();
            });
        });
    }).get();
    BOOST_REQUIRE_EQUAL(expected , actual);
}

SEASTAR_THREAD_TEST_CASE(tmp_file_test_with_path) {
    size_t expected = ~0;
    size_t actual = 0;

    tmp_file::do_with("/tmp", [&] (file f) {
        return do_with(temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment()), [&, f] (auto& buf) mutable {
            expected = buf.size();
            return f.dma_write(0, buf.get(), buf.size()).then([&] (size_t written) {
                actual = written;
                return make_ready_future<>();
            });
        });
    }).get();
    BOOST_REQUIRE_EQUAL(expected , actual);
}

SEASTAR_THREAD_TEST_CASE(tmp_dir_test) {
    size_t expected;
    size_t actual;
    tmp_dir::do_with([&] (compat::filesystem::path p) {
        return tmp_file::do_with(p, [&] (file f) {
            return do_with(temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment()), [&, f] (auto& buf) mutable {
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

SEASTAR_THREAD_TEST_CASE(tmp_dir_test_with_path) {
    size_t expected;
    size_t actual;
    tmp_dir::do_with(".", [&] (compat::filesystem::path p) {
        return tmp_file::do_with(p, [&] (file f) {
            return do_with(temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment()), [&, f] (auto& buf) mutable {
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

SEASTAR_THREAD_TEST_CASE(tmp_dir_test_with_non_existing_path) {
    BOOST_REQUIRE_EXCEPTION(tmp_dir::do_with(tmp_name("/tmp/XXXXXX").get0(), [] (compat::filesystem::path p) {}).get(),
            std::system_error, testing::exception_predicate::message_contains("No such file or directory"));
}

SEASTAR_TEST_CASE(tmp_dir_with_thread_test) {
    return tmp_dir::do_with_thread([&] (compat::filesystem::path p) {
        auto tmp = tmp_file();
        auto f = tmp.open(p).get0();
        auto buf = temporary_buffer<char>::aligned(f.memory_dma_alignment(), f.memory_dma_alignment());
        auto expected = buf.size();
        auto actual = f.dma_write(0, buf.get(), buf.size()).get0();
        BOOST_REQUIRE_EQUAL(expected, actual);
        tmp.close().get();
        tmp.remove().get();
    });
}

SEASTAR_TEST_CASE(tmp_dir_with_leftovers_test) {
    return tmp_dir::do_with_thread([&] (compat::filesystem::path p) {
        auto tmp = make_tmp_file(p).get0();
        BOOST_REQUIRE(file_exists(tmp.native()).get0());
    });
}
