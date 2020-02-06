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
#include <seastar/util/file.hh>

using namespace seastar;
namespace fs = compat::filesystem;

static future<fs::path> get_tmp_filename() {
    return make_tmp_file().then([] (std::tuple<file, fs::path> r) {
        file f = std::get<0>(r);
        fs::path path = std::get<1>(r);
        return f.close().then([f, path = std::move(path)] {
            return make_ready_future<fs::path>(std::move(path));
        });
    });
}

SEASTAR_THREAD_TEST_CASE(make_tmp_file_test) {
    auto filename1 = get_tmp_filename().get0().native();
    BOOST_REQUIRE(file_exists(filename1).get0());

    auto filename2 = get_tmp_filename().get0().native();
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
    BOOST_REQUIRE_EXCEPTION(tmp_dir::do_with("/tmp/this_name_should_not_exist", [] (compat::filesystem::path p) {}).get(),
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
        // Removing the tmp_file is not strictly needed.
        // Leaving it around is explicitly tested in
        // tmp_dir_with_leftovers_test
        tmp.remove().get();
    });
}

SEASTAR_TEST_CASE(tmp_dir_with_leftovers_test) {
    return tmp_dir::do_with_thread([&] (compat::filesystem::path p) {
        std::tuple<file, fs::path> res = make_tmp_file(p).get0();
        auto f = std::get<0>(res);
        auto path = std::get<1>(res);
        f.close().get();
        BOOST_REQUIRE(file_exists(path.native()).get0());
    });
}

static future<> touch_file(const sstring& filename, open_flags oflags = open_flags::rw | open_flags::create) {
    return open_file_dma(filename, oflags).then([] (file f) {
        return f.close().finally([f] {});
    });
}

SEASTAR_TEST_CASE(same_file_test) {
    return tmp_dir::do_with_thread([&] (fs::path p) {
        sstring f1 = (p / "testfile1.tmp").native();
        sstring f2 = (p / "testfile2.tmp").native();

        // same_file should fail when f1 does not exist
        BOOST_REQUIRE_EXCEPTION(same_file(f1, f1).get0(), std::system_error,
                testing::exception_predicate::message_contains("filesystem error: stat failed: No such file or directory"));

        // f1 is same file as itself
        touch_file(f1).get();
        BOOST_REQUIRE(same_file(f1, f1).get0());

        // same_file should fail when f2 does not exist
        BOOST_REQUIRE_EXCEPTION(same_file(f1, f2).get0(), std::system_error,
                testing::exception_predicate::message_contains("filesystem error: stat failed: No such file or directory"));

        // f1 is not same file as newly created f2
        touch_file(f2).get();
        BOOST_REQUIRE(!same_file(f1, f2).get0());

        // f1 and f2 refer to same file when hard-linked
        remove_file(f2).get();
        link_file(f1, f2).get();
        BOOST_REQUIRE(same_file(f1, f2).get0());

        // same_file should fail when f1 does not exist
        remove_file(f1).get();
        BOOST_REQUIRE_EXCEPTION(same_file(f1, f2).get0(), std::system_error,
                testing::exception_predicate::message_contains("filesystem error: stat failed: No such file or directory"));

        // same_file should fail when both f1 and f2 do not exist
        remove_file(f2).get();
        BOOST_REQUIRE_EXCEPTION(same_file(f1, f2).get0(), std::system_error,
                testing::exception_predicate::message_contains("filesystem error: stat failed: No such file or directory"));
    });
}
