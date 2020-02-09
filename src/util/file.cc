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

static future<std::tuple<stat_data, stat_data>> stat_files(sstring path1, sstring path2, follow_symlink fs) {
    // Until file_stat is made noexcept, use the when_all variant that takes lambdas.
    auto fstat = [fs] (sstring p) {
        return file_stat(std::move(p), fs);
    };
    return when_all(fstat(std::move(path1)), fstat(std::move(path2)))
            .then([] (std::tuple<future<stat_data>, future<stat_data>> res) {
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

future<> link_file_ext(sstring oldpath, sstring newpath, allow_overwrite flag) {
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
            return same_file(oldpath, newpath, follow_symlink::no).then([oldpath = std::move(oldpath), newpath = std::move(newpath), flag] (bool same) {
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

future<> rename_file_ext(sstring oldpath, sstring newpath, allow_overwrite flag) {
    return stat_files(oldpath, newpath, follow_symlink::no)
            .then([oldpath = std::move(oldpath), newpath = std::move(newpath), flag] (std::tuple<stat_data, stat_data> res) {
        const stat_data& sd1 = std::get<0>(res);
        const stat_data& sd2 = std::get<1>(res);
        auto same = is_same_file(sd1, sd2);
        int error = 0;
        if (sd1.type == directory_entry_type::directory) {
            if (sd2.type != directory_entry_type::directory) {
                error = ENOTDIR;
            }
        } else {
            if (sd2.type == directory_entry_type::directory) {
                error = EISDIR;
            } else if (flag == allow_overwrite::never ||
                    (flag == allow_overwrite::if_same && !same) ||
                    (flag == allow_overwrite::if_not_same && same)) {
                error = EEXIST;
            }
        }
        if (error) {
            return make_exception_future<>(make_filesystem_error("rename failed", fs::path(oldpath), fs::path(newpath), error));
        }
        if (same) {
            // if newpath refers to the same file as oldpath,
            // just remove oldpath to complete the operation.
            return remove_file(oldpath);
        }
        // otherwise retry the rename (that will overwrite newpath using the regular rename(2) semantics)
        return rename_file(std::move(oldpath), std::move(newpath));
    });
}

} //namespace seastar
