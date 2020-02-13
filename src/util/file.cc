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

#include <list>

#include <seastar/core/seastar.hh>
#include <seastar/core/file.hh>
#include <seastar/util/file.hh>
#include <seastar/util/exceptions.hh>

namespace seastar {

namespace fs = compat::filesystem;

static future<> _recursive_remove_directory(const fs::path path) {
    return do_with(std::list<sstring>(), std::list<sstring>(), std::move(path),
            [] (std::list<sstring>& files, std::list<sstring>& dirs, const fs::path& path) {
        return open_directory(path.native()).then([&] (file dir) {
            auto lister = dir.list_directory([&] (directory_entry de) mutable {
                auto& vec = de.type && (*de.type == directory_entry_type::directory) ? dirs : files;
                vec.push_back(de.name);
                return make_ready_future<>();
            });
            return lister.done().then([dir] () mutable {
                return dir.close().finally([dir] {});
            }).then([&files, &path] {
                return parallel_for_each(files, [&path] (auto& filename) {
                    return remove_file((path / filename.c_str()).native());
                });
            }).then([&dirs, &path] {
                return parallel_for_each(dirs, [&path] (auto& dirname) {
                    return _recursive_remove_directory((path / dirname.c_str()).native());
                });
            });
        }).then([&path] {
            return remove_file(path.native());
        });
    });
}

future<> recursive_remove_directory(fs::path path) {
    std::optional<fs::path> parent_path;
    if (path.has_parent_path()) {
        parent_path.emplace(path.parent_path());
    }
    return _recursive_remove_directory(path).then([parent_path = std::move(parent_path)] {
        if (parent_path) {
            return sync_directory(parent_path->native());
        } else {
            return make_ready_future<>();
        }
    });
}

} //namespace seastar
