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

#include <random>

#include <seastar/core/seastar.hh>
#include <seastar/core/file.hh>
#include <seastar/util/file.hh>
#include <seastar/util/exceptions.hh>

namespace seastar {

namespace fs = compat::filesystem;

future<> recursive_remove_directory(fs::path path) {
    return open_directory(path.native()).then([path] (file f) {
        auto lister = f.list_directory([parent = std::move(path)] (directory_entry de) {
            auto path = parent / de.name.c_str();
            if (!de.type) {
                return make_exception_future<>(make_filesystem_error("Cannot remove directory with unknown file type", path, EEXIST));
            }
            if (*de.type == directory_entry_type::directory) {
                return recursive_remove_directory(std::move(path));
            } else {
                return remove_file(path.native());
            }
        });
        return lister.done().then([f] () mutable {
            return f.flush();
        }).finally([f] () mutable {
            return f.close().finally([f] {});
        });
    }).then([path] {
        return remove_file(path.native());
    }).then([path] {
        auto parent_path = path.has_parent_path() ? path.parent_path().native() : ".";
        return sync_directory(std::move(parent_path));
    });
}

} //namespace seastar
