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
#include <seastar/testing/test_runner.hh>

#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/print.hh>
#include <seastar/util/tmp_file.hh>
#include <seastar/util/file.hh>

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

static future<> touch_file(const sstring& filename, open_flags oflags = open_flags::rw | open_flags::create) {
    return open_file_dma(filename, oflags).then([] (file f) {
        return f.close().finally([f] {});
    });
}

SEASTAR_THREAD_TEST_CASE(test_recursive_remove_directory) {
    struct test_dir {
        test_dir *parent;
        sstring name;
        std::list<sstring> sub_files = {};
        std::list<test_dir> sub_dirs = {};

        test_dir(test_dir* parent, sstring name)
            : parent(parent)
            , name(std::move(name))
        { }

        fs::path path() const {
            if (!parent) {
                return fs::path(name.c_str());
            }
            return parent->path() / name.c_str();
        }

        void fill_random_file(std::uniform_int_distribution<unsigned>& dist, std::default_random_engine& eng) {
            sub_files.emplace_back(format("file-{}", dist(eng)));
        }

        test_dir& fill_random_dir(std::uniform_int_distribution<unsigned>& dist, std::default_random_engine& eng) {
            sub_dirs.emplace_back(this, format("dir-{}", dist(eng)));
            return sub_dirs.back();
        }

        void random_fill(int level, int levels, std::uniform_int_distribution<unsigned>& dist, std::default_random_engine& eng) {
            int num_files = dist(eng) % 10;
            int num_dirs = (level < levels - 1) ? (1 + dist(eng) % 3) : 0;

            for (int i = 0; i < num_files; i++) {
                fill_random_file(dist, eng);
            }

            if (num_dirs) {
                level++;
                for (int i = 0; i < num_dirs; i++) {
                    fill_random_dir(dist, eng).random_fill(level, levels, dist, eng);
                }
            }
        }

        future<> populate() {
            return touch_directory(path().native()).then([this] {
                return parallel_for_each(sub_files, [this] (auto& name) {
                    return touch_file((path() / name.c_str()).native());
                }).then([this] {
                    return parallel_for_each(sub_dirs, [this] (auto& sub_dir) {
                        return sub_dir.populate();
                    });
                });
            });
        }
    };

    auto& eng = testing::local_random_engine;
    auto dist = std::uniform_int_distribution<unsigned>();
    int levels = 1 + dist(eng) % 3;
    test_dir root = { nullptr, "/tmp" };
    test_dir base = { &root, format("base-{}", dist(eng)) };
    base.random_fill(0, levels, dist, eng);
    base.populate().get();
    recursive_remove_directory(base.path()).get();
    BOOST_REQUIRE(!file_exists(base.path().native()).get0());
}
