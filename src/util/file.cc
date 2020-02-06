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
 * Copyright 2020 ScyllaDB
 */

#include <deque>

#include <seastar/core/future-util.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/file.hh>
#include <seastar/core/semaphore.hh>
#include <seastar/util/file.hh>
#include <seastar/util/exceptions.hh>
#include <seastar/util/std-compat.hh>

namespace seastar {

namespace fs = compat::filesystem;

static future<> do_recursive_remove_directory(const fs::path path) {
    struct work_entry {
        const fs::path path;
        bool listed;
        lw_shared_ptr<semaphore> parent;
        lw_shared_ptr<semaphore> sem;

        work_entry(const fs::path path, bool listed, lw_shared_ptr<semaphore> parent = nullptr)
                : path(std::move(path))
                , listed(listed)
                , parent(parent)
                , sem(listed ? nullptr : make_lw_shared<semaphore>(0))
        {
            if (parent) {
                parent->consume();
            }
        }

        work_entry(const work_entry& x) = delete;
        work_entry(work_entry&& x) = default;

        ~work_entry() {
            assert(!sem || !sem->available_units());
        }

        future<> list(std::deque<work_entry>& work_queue) {
            return open_directory(path.native()).then([this, &work_queue] (file dir) mutable {
                return do_with(std::move(dir), [this, &work_queue] (file& dir) mutable {
                    return dir.list_directory([this, &work_queue] (directory_entry de) mutable {
                        if (de.type && *de.type == directory_entry_type::directory) {
                            work_queue.emplace_back(path / de.name.c_str(), false, sem);
                        } else {
                            work_queue.emplace_back(path / de.name.c_str(), true, sem);
                        }
                        return make_ready_future<>();
                    }).done().then([&dir] () mutable {
                        return dir.close();
                    });
                });
            });
        }

        future<> wait_for_children() {
            return sem ? sem->wait(0) : make_ready_future<>();
        }

        void signal_parent() {
            if (parent) {
                parent->signal();
            }
        }

        future<> remove() {
            return wait_for_children().then([this] {
                return remove_file(path.native());
            }).then([this] {
                signal_parent();
            });
        }
    };

    return do_with(std::deque<work_entry>(), [path = std::move(path)] (std::deque<work_entry>& work_queue) {
        work_queue.emplace_back(std::move(path), false);
        auto is_done = [&work_queue] { return work_queue.empty(); };
        return do_until(std::move(is_done), [&work_queue] () mutable {
            return do_with(std::move(work_queue), [&work_queue] (std::deque<work_entry>& work) mutable {
                return parallel_for_each(work, [&work_queue] (work_entry& ent) {
                    if (ent.listed) {
                        return ent.remove();
                    } else {
                        return ent.list(work_queue).then([&work_queue, &ent] () mutable {
                            ent.listed = true;
                            work_queue.push_back(std::move(ent));
                            return make_ready_future<>();
                        });
                    }
                });
            });
        });
    });
}

future<> recursive_remove_directory(fs::path path) {
    return open_directory((path / "..").native()).then([path = std::move(path)] (file parent) {
        return do_with(std::move(parent), [path = std::move(path)] (file& parent) {
            return do_recursive_remove_directory(path).then([&parent] {
                return parent.flush().then([&parent] () mutable {
                    return parent.close();
                });
            });
        });
    });
}

static future<std::tuple<stat_data, stat_data>> stat_files(sstring path1, sstring path2, follow_symlink fs) {
    // Until file_stat is made noexcept, use the when_all variant that takes lambdas.
    auto fstat1 = [p = std::move(path1), fs] {
        return file_stat(std::move(p), fs);
    };
    auto fstat2 = [p = std::move(path2), fs] {
        return file_stat(std::move(p), fs);
    };
    return when_all(std::move(fstat1), std::move(fstat2)).then([] (std::tuple<future<stat_data>, future<stat_data>> res) {
        auto& f1 = std::get<0>(res);
        auto& f2 = std::get<1>(res);
        if (f1.failed()) {
            f2.ignore_ready_future();
            return make_exception_future<std::tuple<stat_data, stat_data>>(f1.get_exception());
        }
        if (f2.failed()) {
            f1.ignore_ready_future();
            return make_exception_future<std::tuple<stat_data, stat_data>>(f2.get_exception());
        }
        auto ret = std::tuple<stat_data, stat_data>(f1.get0(), f2.get0());
        return make_ready_future<std::tuple<stat_data, stat_data>>(std::move(ret));
    });
}

static bool is_same_file(const stat_data& sd1, const stat_data& sd2) {
    return sd1.device_id == sd2.device_id && sd1.inode_number == sd2.inode_number;
}

future<bool> same_file(sstring path1, sstring path2, follow_symlink fs) {
    return stat_files(std::move(path1), std::move(path2), fs)
            .then([] (std::tuple<stat_data, stat_data> res) {
        return make_ready_future<bool>(is_same_file(std::get<0>(res), std::get<1>(res)));
    });
}

} //namespace seastar
