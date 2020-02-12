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
#include <seastar/util/file.hh>

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

static future<std::tuple<file, fs::path>>
open_tmp_file(const fs::path path_template, open_flags oflags, const file_open_options options) {
    oflags |= open_flags::create | open_flags::exclusive;
    return generate_tmp_name(std::move(path_template)).then([oflags, options = std::move(options)] (fs::path path) {
        return open_file_dma(path.native(), oflags, std::move(options)).then([path = std::move(path)] (file f) mutable {
            auto res = std::make_tuple<file, fs::path>(std::move(f), std::move(path));
            return make_ready_future<std::tuple<file, fs::path>>(std::move(res));
        });
    });
}

future<fs::path>
make_tmp_file(const fs::path path_template, open_flags oflags, const file_open_options options) {
    return open_tmp_file(std::move(path_template), oflags, std::move(options)).then([] (std::tuple<file, fs::path> r) {
        return std::get<0>(r).close().then([r = std::move(r)] {
            return make_ready_future<fs::path>(std::get<1>(r));
        });
    });
}

tmp_file::~tmp_file() {
    assert(!opened());
    assert(!has_path());
}

future<file> tmp_file::open(const fs::path path_template, open_flags oflags, const file_open_options options) {
    assert(!opened());
    return open_tmp_file(std::move(path_template), oflags, std::move(options)).then([this] (std::tuple<file, fs::path> r) {
        _path = std::get<1>(r);
        auto& f = std::get<0>(r);
        _file.emplace(f);
        return make_ready_future<file>(f);
    });
}

future<> tmp_file::close() {
    if (!opened()) {
        return make_ready_future<>();
    }
    return _file->close().then([this] {
        _file = compat::nullopt;
    });
}

future<> tmp_file::remove() {
    if (!has_path()) {
        return make_ready_future<>();
    }
    return remove_file(get_path().native()).then([this] {
        _path.clear();
    });
}

tmp_dir::~tmp_dir() {
    assert(!has_path());
}

future<fs::path> tmp_dir::create(const fs::path path_template, file_permissions create_permissions) {
    assert(!has_path());
    return generate_tmp_name(std::move(path_template)).then([this, create_permissions] (fs::path path) {
        return touch_directory(path.native(), create_permissions).then([this, path = std::move(path)] {
            _path = path;
            return make_ready_future<fs::path>(std::move(path));
        });
    });
}

future<> tmp_dir::remove() {
    if (!has_path()) {
        return make_ready_future<>();
    }
    return recursive_remove_directory(get_path()).then([this] {
        _path.clear();
    });
}

} //namespace seastar
