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
#include <seastar/util/file.hh>
#include <seastar/util/exceptions.hh>
#include <seastar/util/std-compat.hh>

namespace seastar {

namespace fs = compat::filesystem;

static future<> do_recursive_remove_directory(const fs::path path) {
    struct work_entry {
        const fs::path path;
        bool listed;

        work_entry(const fs::path path, bool listed)
                : path(std::move(path))
                , listed(listed)
        {
        }

        work_entry(const work_entry& x) = delete;
        work_entry(work_entry&& x) = default;

        ~work_entry() = default;

        future<> list(std::deque<work_entry>& work_queue) {
            return open_directory(path.native()).then([this, &work_queue] (file dir) mutable {
                return do_with(std::move(dir), [this, &work_queue] (file& dir) mutable {
                    return dir.list_directory([this, &work_queue] (directory_entry de) mutable {
                        if (de.type && *de.type == directory_entry_type::directory) {
                            work_queue.emplace_back(path / de.name.c_str(), false);
                        } else {
                            work_queue.emplace_back(path / de.name.c_str(), true);
                        }
                        return make_ready_future<>();
                    }).done().then([&dir] () mutable {
                        return dir.close();
                    });
                });
            });
        }

        future<> remove() {
            return remove_file(path.native());
        }
    };

    return do_with(std::deque<work_entry>(), [path = std::move(path)] (std::deque<work_entry>& work_queue) {
        work_queue.emplace_back(std::move(path), false);
        auto is_done = [&work_queue] { return work_queue.empty(); };
        return do_until(std::move(is_done), [&work_queue] () mutable {
            return do_with(std::move(work_queue), [&work_queue] (std::deque<work_entry>& work) mutable {
                return do_for_each(work, [&work_queue] (work_entry& ent) {
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

} //namespace seastar
