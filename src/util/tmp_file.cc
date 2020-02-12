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

#include <iostream>
#include <random>

#include <seastar/core/seastar.hh>
#include <seastar/util/exceptions.hh>
#include <seastar/util/std-compat.hh>
#include <seastar/util/tmp_file.hh>

namespace seastar {

namespace fs = compat::filesystem;

static future<fs::path>
generate_tmp_name(const fs::path path_template) {
    fs::path path = path_template.parent_path();
    std::string filename = path_template.filename().native();
    if (path.empty()) {
        path = ".";
    }
    auto pos = filename.find("XX");
    if (pos == std::string::npos) {
        path /= filename;
        filename = default_tmp_name_template;
        pos = filename.find("XX");
        assert(pos != std::string::npos);
    }
    auto end = filename.size();
    static const std::string charset("0123456789abcdef");
    static thread_local std::default_random_engine engine(std::random_device{}());
    static thread_local std::uniform_int_distribution<int> dist(0, charset.size() - 1);
    while (pos < end && filename[pos] == 'X') {
        filename[pos++] = charset[dist(engine)];
    }
    return file_stat(path.native())
            .then([path = std::move(path), filename = std::move(filename)] (stat_data sd) {
        if (sd.type != directory_entry_type::directory) {
            return make_exception_future<fs::path>(make_filesystem_error("Cannot make temporary name", path, ENOTDIR));
        }
        return make_ready_future<fs::path>(path / filename);
    });
}

future<fs::path>
tmp_name(const fs::path path_template) {
    return generate_tmp_name(std::move(path_template)).then([] (fs::path path) mutable {
        return file_exists(path.native()).then([path = std::move(path)] (bool exists) {
            if (__builtin_expect(exists, false)) {
                return make_exception_future<fs::path>(make_filesystem_error("Temporary name already exists", path, EEXIST));
            }
            return make_ready_future<fs::path>(std::move(path));
        });
    });
}

} //namespace seastar
