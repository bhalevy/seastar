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

static bool _is_same_file(const stat_data& sd1, const stat_data& sd2) {
    return sd1.device_id == sd2.device_id && sd1.inode_number == sd2.inode_number;
}

future<bool>
same_file(sstring path1, sstring path2, follow_symlink fs) {
    return when_all(file_stat(std::move(path1), fs), file_stat(std::move(path2), fs))
            .then([] (std::tuple<future<stat_data>, future<stat_data>> res) {
        auto& f1 = std::get<0>(res);
        auto& f2 = std::get<1>(res);
        if (f1.failed()) {
            f2.ignore_ready_future();
            return make_exception_future<bool>(f1.get_exception());
        }
        if (f2.failed()) {
            f1.ignore_ready_future();
            return make_exception_future<bool>(f2.get_exception());
        }
        return make_ready_future<bool>(_is_same_file(f1.get0(), f2.get0()));
    });
}

static future<>
_link_file_ext(sstring oldpath, sstring newpath, allow_overwrite flag, bool is_retry) {
    return link_file(oldpath, newpath)
            .handle_exception([oldpath = std::move(oldpath), newpath = std::move(newpath), flag, is_retry] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (const std::system_error& e) {
            auto error = e.code().value();
            if (error != EEXIST || flag == allow_overwrite::never || is_retry) {
                return make_exception_future<>(make_filesystem_error("link failed", fs::path(oldpath), fs::path(newpath), error));
            }
            return when_all(file_stat(oldpath, follow_symlink::no), file_stat(newpath, follow_symlink::no))
                    .then([oldpath = std::move(oldpath), newpath = std::move(newpath), flag] (std::tuple<future<stat_data>, future<stat_data>> res) {
                auto& f1 = std::get<0>(res);
                auto& f2 = std::get<1>(res);
                if (f1.failed()) {
                    f2.ignore_ready_future();
                    return make_exception_future<>(f1.get_exception());
                }
                if (f2.failed()) {
                    f1.ignore_ready_future();
                    return make_exception_future<>(f2.get_exception());
                }
                const stat_data& sd1 = f1.get0();
                const stat_data& sd2 = f2.get0();
                if (sd2.type == directory_entry_type::directory) {
                    return make_exception_future<>(make_filesystem_error("link failed", fs::path(oldpath), fs::path(newpath), EISDIR));
                }
                auto same = _is_same_file(sd1, sd2);
                if ((same && flag == allow_overwrite::if_not_same) || (!same && flag == allow_overwrite::if_same)) {
                    return make_exception_future<>(make_filesystem_error("link failed", fs::path(oldpath), fs::path(newpath), EEXIST));
                } else if (!same) {
                    // retry after removing new_file as permitted by allow_overwrite
                    return remove_file(newpath).then([oldpath = std::move(oldpath), newpath = std::move(newpath), flag] {
                        return _link_file_ext(std::move(oldpath), std::move(newpath), flag, true /* retry */);
                    });
                }
                return make_ready_future<>();
            });
        }
    });
}

future<>
link_file_ext(sstring oldpath, sstring newpath, allow_overwrite flag) {
    return link_file(oldpath, newpath)
            .handle_exception([oldpath = std::move(oldpath), newpath = std::move(newpath), flag] (std::exception_ptr eptr) {
        try {
            std::rethrow_exception(eptr);
        } catch (const std::system_error& e) {
            auto error = e.code().value();
            // Any error other than EEXIST is returned
            // allow_overwrite::never provides exactly the same semantics as link(2)
            if (error != EEXIST || flag == allow_overwrite::never) {
                return make_exception_future<>(make_filesystem_error("link failed", fs::path(oldpath), fs::path(newpath), error));
            }
            // See if oldpath and newpath are hard links to the same file
            return when_all(file_stat(oldpath, follow_symlink::no), file_stat(newpath, follow_symlink::no))
                    .then([oldpath = std::move(oldpath), newpath = std::move(newpath), flag] (std::tuple<future<stat_data>, future<stat_data>> res) {
                auto& f1 = std::get<0>(res);
                auto& f2 = std::get<1>(res);
                if (f1.failed()) {
                    f2.ignore_ready_future();
                    return make_exception_future<>(f1.get_exception());
                }
                if (f2.failed()) {
                    f1.ignore_ready_future();
                    return make_exception_future<>(f2.get_exception());
                }
                const stat_data& sd1 = f1.get0();
                const stat_data& sd2 = f2.get0();
                auto same = _is_same_file(sd1, sd2);
                if ((flag == allow_overwrite::if_same && !same) ||
                    (flag == allow_overwrite::if_not_same && same)) {
                    return make_exception_future<>(make_filesystem_error("link failed", fs::path(oldpath), fs::path(newpath), EEXIST));
                }
                if (same) {
                    // if newpath is already linked to the same inode as oldpath, we're done
                    return make_ready_future<>();
                }
                // retry after removing new_file as permitted by allow_overwrite
                return remove_file(newpath).then([oldpath = std::move(oldpath), newpath = std::move(newpath), flag] {
                    return link_file(std::move(oldpath), std::move(newpath));
                });
            });
        }
    });
}

} //namespace seastar
