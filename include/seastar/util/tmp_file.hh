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

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/file.hh>
#include <seastar/util/std-compat.hh>

namespace seastar {

static constexpr const char* default_tmp_name_template = "XXXXXX.tmp";
static constexpr const char* default_tmp_path = "/tmp";

/// Returns a valid file exclusively created by the function and its pathname.
///
/// \param path_template - path where the file is to be created,
///                        optionally including a template for the file name.
///
/// \note
///    path_template may optionally include a filename template in the last component of the path.
///    The template is indicated by two or more consecutive XX's.
///    Those will be replaced in the result path by a unique string.
///
///    If no filename template is found, then path_template is assumed to refer to the directory where
///    the temporary file is to be created at (a.k.a. the parent directory) and `default_tmp_name_template`
///    is appended to the path as the filename template.
///
///    The parent directory must exist and be writable to the current process.
///
future<std::tuple<file, compat::filesystem::path>> make_tmp_file(const compat::filesystem::path path_template = default_tmp_path,
        open_flags oflags = open_flags::rw, const file_open_options options = {});

class tmp_file {
    compat::filesystem::path _path;
    compat::optional<file> _file;

public:
    tmp_file() = default;
    tmp_file(const tmp_file&) = delete;
    tmp_file(tmp_file&& x) : _path(), _file() {
        std::swap(x._path, _path);
        std::swap(x._file, _file);
    }

    ~tmp_file();

    future<file> open(const compat::filesystem::path path_template = default_tmp_path, open_flags oflags = open_flags::rw, const file_open_options options = {});
    future<> close();
    future<> remove();

    template <typename Func>
    static future<> do_with(const compat::filesystem::path path_template, Func&& func,
            open_flags oflags = open_flags::rw,
            const file_open_options options = {}) {
        return seastar::do_with(tmp_file(), [func = std::move(func), path_template = std::move(path_template), oflags, options = std::move(options)] (tmp_file& t) {
            return t.open(std::move(path_template), oflags, std::move(options)).then([func = std::move(func)] (file f) {
                return func(std::move(f));
            }).finally([&t] {
                return t.close().finally([&t] {
                    return t.remove();
                });
            });
        });
    }

    template <typename Func>
    static future<> do_with(Func&& func) {
        return do_with(default_tmp_path, std::move(func));
    }

    bool has_path() const {
        return !_path.empty();
    }

    bool opened() const {
        return (bool)_file;
    }

    const compat::filesystem::path& get_path() const {
        return _path;
    }

    file get_file() {
        if (!opened()) {
            throw std::runtime_error("tmp_file not opened");
        }
        return *_file;
    }
};

class tmp_dir {
    compat::filesystem::path _path;

public:
    tmp_dir() = default;
    tmp_dir(const tmp_dir&) = delete;
    tmp_dir(tmp_dir&& x) : _path() {
        std::swap(x._path, _path);
    }

    ~tmp_dir();

    future<compat::filesystem::path> create(const compat::filesystem::path path_template = default_tmp_path,
            file_permissions create_permissions = file_permissions::default_dir_permissions);
    future<> remove();

    template <typename Func>
    static future<> do_with(const compat::filesystem::path path_template, Func&& func,
            file_permissions create_permissions = file_permissions::default_dir_permissions) {
        return seastar::do_with(tmp_dir(), [func = std::move(func), path_template = std::move(path_template), create_permissions] (tmp_dir& t) {
            return t.create(std::move(path_template), create_permissions).then([func = std::move(func)] (compat::filesystem::path p) {
                return func(std::move(p));
            }).finally([&t] {
                return t.remove();
            });
        });
    }

    template <typename Func>
    static future<> do_with(Func&& func) {
        return do_with(default_tmp_path, std::move(func));
    }

    bool has_path() const {
        return !_path.empty();
    }

    const compat::filesystem::path& get_path() const {
        return _path;
    }
};

} // namespace seastar
