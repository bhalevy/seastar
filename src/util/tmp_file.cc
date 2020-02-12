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

static fs::path
generate_tmp_name(const fs::path path_template) {
    fs::path parent = path_template.parent_path();
    std::string filename = path_template.filename().native();
    if (parent.empty()) {
        parent = ".";
    }
    auto pos = filename.find("XX");
    if (pos == std::string::npos) {
        parent = path_template;
        filename = default_tmp_name_template;
        pos = filename.find("XX");
        assert(pos != std::string::npos);
    }
    auto end = filename.size();
    static constexpr char charset[] = "0123456789abcdef";
    static thread_local std::default_random_engine engine(std::random_device{}());
    static thread_local std::uniform_int_distribution<int> dist(0, sizeof(charset) - 2);
    while (pos < end && filename[pos] == 'X') {
        filename[pos++] = charset[dist(engine)];
    }
    parent /= filename;
    return parent;
}

tmp_file::tmp_file(tmp_file&& x) noexcept
    : _path(std::move(x._path))
    , _file(std::move(x._file))
{
    std::swap(_is_open, x._is_open);
}

tmp_file::~tmp_file() {
    assert(!has_path());
    assert(!is_open());
}

future<> tmp_file::open(const fs::path path_template, open_flags oflags, const file_open_options options) {
    assert(!has_path());
    assert(!is_open());
    oflags |= open_flags::create | open_flags::exclusive;
    _path = generate_tmp_name(std::move(path_template));
    return open_file_dma(_path.native(), oflags, std::move(options)).then([this] (file f) mutable {
        _file = std::move(f);
        _is_open = true;
        return make_ready_future<>();
    });
}

future<> tmp_file::close() {
    if (!is_open()) {
        return make_ready_future<>();
    }
    return _file.close().then([this] {
        _is_open = false;
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

future<tmp_file>
make_tmp_file(const fs::path path_template, open_flags oflags, const file_open_options options) {
    return do_with(tmp_file(), [path_template = std::move(path_template), oflags, options = std::move(options)] (tmp_file& t) {
        return t.open(std::move(path_template), oflags, std::move(options)).then([&t] {
            return make_ready_future<tmp_file>(std::move(t));
        });
    });
}

} //namespace seastar
