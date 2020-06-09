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

#include <seastar/testing/test_case.hh>
#include <seastar/testing/thread_test_case.hh>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/tmp_file.hh>
#include <seastar/util/filesystem_error_injector.hh>

#ifdef SEASTAR_ENABLE_FILESYSTEM_ERROR_INJECTION

using namespace seastar;
namespace fs = std::filesystem;
namespace fsei = filesystem_error_injector;

// must be called in a seastar thread
void try_open_file_dma(sstring filename, open_flags flags) {
    auto f = open_file_dma(filename, open_flags::rw | open_flags::create).get0();
    f.close().get();
}

SEASTAR_TEST_CASE(test_open_injector) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        sstring filename = (t.get_path() / "testfile.tmp").native();
        fsei::injector injector(fsei::syscall_type::open, EIO);

        // test single failure
        injector.fail_once();
        try {
            try_open_file_dma(filename, open_flags::rw | open_flags::create);
            BOOST_REQUIRE(false);
        } catch (const fs::filesystem_error& e) {
            BOOST_REQUIRE(e.code().value() == EIO);
        }
        BOOST_REQUIRE(!file_exists(filename).get0());

        // retrying should succeed
        try_open_file_dma(filename, open_flags::rw | open_flags::create);
        BOOST_REQUIRE(file_exists(filename).get0());

        // test delayed, multiple failures
        injector.fail_after(1, 2);
        // expect first open to succeed, then get 2 failures, then succeed again
        try_open_file_dma(filename, open_flags::ro);
        BOOST_REQUIRE_THROW(try_open_file_dma(filename, open_flags::ro), fs::filesystem_error);
        BOOST_REQUIRE_THROW(try_open_file_dma(filename, open_flags::ro), fs::filesystem_error);
        try_open_file_dma(filename, open_flags::ro);
    });
}

SEASTAR_TEST_CASE(test_open_injector_with_guard) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        sstring filename = (t.get_path() / "testfile.tmp").native();
        fsei::injector injector(fsei::syscall_type::open, EIO);

        // fail from now on
        injector.fail();
        try {
            try_open_file_dma(filename, open_flags::rw | open_flags::create);
            BOOST_REQUIRE(false);
        } catch (const fs::filesystem_error& e) {
            BOOST_REQUIRE(e.code().value() == EIO);
        }
        BOOST_REQUIRE(!file_exists(filename).get0());

        // retrying with guard should succeed
        {
            fsei::disable_guard guard;
            try_open_file_dma(filename, open_flags::rw | open_flags::create);
            BOOST_REQUIRE(file_exists(filename).get0());
        };

        try {
            try_open_file_dma(filename, open_flags::rw | open_flags::create);
            BOOST_REQUIRE(false);
        } catch (const fs::filesystem_error& e) {
            BOOST_REQUIRE(e.code().value() == EIO);
        }
    });
}

SEASTAR_TEST_CASE(test_open_path_based_injector) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        sstring filename1 = (t.get_path() / "testfile1.tmp").native();
        sstring filename2 = (t.get_path() / "testfile2.tmp").native();
        fsei::injector injector(fsei::syscall_type::open, EIO, [test_path = filename1] (std::optional<sstring> path, std::optional<sstring>, ulong) {
            return *path == test_path;
        });

        // opening filename1 should fail
        injector.fail();
        try {
            try_open_file_dma(filename1, open_flags::rw | open_flags::create);
            BOOST_REQUIRE(false);
        } catch (const fs::filesystem_error& e) {
            BOOST_REQUIRE(e.code().value() == EIO);
        }
        BOOST_REQUIRE(!file_exists(filename1).get0());

        // opening filename2 should succeed
        try_open_file_dma(filename2, open_flags::rw | open_flags::create);
        BOOST_REQUIRE(file_exists(filename2).get0());

        // and again
        BOOST_REQUIRE_THROW(open_file_dma(filename1, open_flags::rw | open_flags::create).get0(), fs::filesystem_error);
        try_open_file_dma(filename2, open_flags::ro);
    });
}

SEASTAR_TEST_CASE(test_all_injector) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        sstring filename = (t.get_path() / "testfile.tmp").native();
        fsei::injector injector(fsei::syscall_type::all, ENOSPC);

        injector.fail_after(1);
        auto f = open_file_dma(filename, open_flags::rw | open_flags::create).get0();

        size_t aligned_size = 4096;
        auto buf = allocate_aligned_buffer<unsigned char>(aligned_size, aligned_size);
        std::fill(buf.get(), buf.get() + aligned_size, 0);
        try {
            f.dma_write(0, buf.get(), aligned_size).get();
            BOOST_REQUIRE(false);
        } catch (const std::system_error& e) {
            BOOST_REQUIRE(e.code().value() == ENOSPC);
        } catch (const std::exception& e) {
            std::cerr << "unexpected exception from dma_write: " << e;
            BOOST_REQUIRE(false);
        }
        try {
            f.dma_read(0, buf.get(), aligned_size).get();
            BOOST_REQUIRE(false);
        } catch (const std::system_error& e) {
            BOOST_REQUIRE(e.code().value() == ENOSPC);
        } catch (const std::exception& e) {
            std::cerr << "unexpected exception from dma_read: " << e;
            BOOST_REQUIRE(false);
        }

        f.close().get();

        BOOST_REQUIRE_THROW(remove_file(filename).get(), fs::filesystem_error);
        injector.cancel();
        remove_file(filename).get();
        BOOST_REQUIRE(!file_exists(filename).get0());
    });
}

SEASTAR_TEST_CASE(test_write_like_injector) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        sstring filename = (t.get_path() / "testfile.tmp").native();
        fsei::injector injector(fsei::syscall_type::write_like, ENOSPC);

        injector.fail_after(1);
        auto f = open_file_dma(filename, open_flags::rw | open_flags::create).get0();

        size_t aligned_size = 4096;
        auto buf = allocate_aligned_buffer<unsigned char>(aligned_size, aligned_size);
        std::fill(buf.get(), buf.get() + aligned_size, 0);
        try {
            f.dma_write(0, buf.get(), aligned_size).get();
            BOOST_REQUIRE(false);
        } catch (const std::system_error& e) {
            BOOST_REQUIRE(e.code().value() == ENOSPC);
        } catch (const std::exception& e) {
            std::cerr << "unexpected exception from dma_write: " << e;
            BOOST_REQUIRE(false);
        }
        BOOST_REQUIRE_EQUAL(f.dma_read(0, buf.get(), aligned_size).get0(), 0);

        f.close().get();

        BOOST_REQUIRE_THROW(remove_file(filename).get(), fs::filesystem_error);
        injector.cancel();
        remove_file(filename).get();
        BOOST_REQUIRE(!file_exists(filename).get0());
    });
}

SEASTAR_TEST_CASE(test_read_like_injector) {
    return tmp_dir::do_with_thread([] (tmp_dir& t) {
        sstring filename = (t.get_path() / "testfile.tmp").native();
        fsei::injector injector(fsei::syscall_type::read_like, EMFILE);

        injector.fail_after(1);
        auto f = open_file_dma(filename, open_flags::rw | open_flags::create).get0();

        size_t aligned_size = 4096;
        auto buf = allocate_aligned_buffer<unsigned char>(aligned_size, aligned_size);
        std::fill(buf.get(), buf.get() + aligned_size, 0);
        BOOST_REQUIRE_EQUAL(f.dma_write(0, buf.get(), aligned_size).get0(), aligned_size);
        try {
            f.dma_read(0, buf.get(), aligned_size).get();
            BOOST_REQUIRE(false);
        } catch (const std::system_error& e) {
            BOOST_REQUIRE(e.code().value() == EMFILE);
        } catch (const std::exception& e) {
            std::cerr << "unexpected exception from dma_read: " << e;
            BOOST_REQUIRE(false);
        }

        f.close().get();

        remove_file(filename).get();
        injector.cancel();
        BOOST_REQUIRE(!file_exists(filename).get0());
    });
}

#endif // SEASTAR_ENABLE_FILESYSTEM_ERROR_INJECTION
