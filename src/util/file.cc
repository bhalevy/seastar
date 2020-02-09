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
#include <list>
#include <deque>

#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/util/file.hh>
#include <seastar/util/exceptions.hh>

namespace seastar {

namespace fs = compat::filesystem;

future<> make_directory(sstring name, file_permissions permissions) noexcept {
    return engine().make_directory(std::move(name), permissions);
}

future<> touch_directory(sstring name, file_permissions permissions) noexcept {
    return engine().touch_directory(std::move(name), permissions);
}

future<> sync_directory(sstring name) noexcept {
    return open_directory(std::move(name)).then([] (file f) {
        return do_with(std::move(f), [] (file& f) {
            return f.flush().then([&f] () mutable {
                return f.close();
            });
        });
    });
}

static future<> do_recursive_touch_directory(sstring base, sstring name, file_permissions permissions) noexcept {
    static const sstring::value_type separator = '/';

    if (name.empty()) {
        return make_ready_future<>();
    }

    size_t pos = std::min(name.find(separator), name.size() - 1);
    base += name.substr(0 , pos + 1);
    name = name.substr(pos + 1);
    if (name.length() == 1 && name[0] == separator) {
        name = {};
    }
    // use the optional permissions only for last component,
    // other directories in the patch will always be created using the default_dir_permissions
    auto f = name.empty() ? touch_directory(base, permissions) : touch_directory(base);
    return f.then([=] {
        return do_recursive_touch_directory(base, std::move(name), permissions);
    }).then([base] {
        // We will now flush the directory that holds the entry we potentially
        // created. Technically speaking, we only need to touch when we did
        // create. But flushing the unchanged ones should be cheap enough - and
        // it simplifies the code considerably.
        if (base.empty()) {
            return make_ready_future<>();
        }

        return sync_directory(base);
    });
}

future<> recursive_touch_directory(sstring name, file_permissions permissions) noexcept {
    // If the name is empty,  it will be of the type a/b/c, which should be interpreted as
    // a relative path. This means we have to flush our current directory
    sstring base = "";
    if (name[0] != '/' || name[0] == '.') {
        base = "./";
    }
    return do_recursive_touch_directory(std::move(base), std::move(name), permissions);
}

future<> remove_file(sstring pathname) noexcept {
    return engine().remove_file(std::move(pathname));
}

future<> rename_file(sstring old_pathname, sstring new_pathname) noexcept {
    return engine().rename_file(std::move(old_pathname), std::move(new_pathname));
}

future<fs_type> file_system_at(sstring name) noexcept {
    return engine().file_system_at(std::move(name));
}

future<uint64_t> fs_avail(sstring name) noexcept {
    return engine().statvfs(std::move(name)).then([] (struct statvfs st) {
        return make_ready_future<uint64_t>(st.f_bavail * st.f_frsize);
    });
}

future<uint64_t> fs_free(sstring name) noexcept {
    return engine().statvfs(std::move(name)).then([] (struct statvfs st) {
        return make_ready_future<uint64_t>(st.f_bfree * st.f_frsize);
    });
}

future<stat_data> file_stat(sstring name, follow_symlink follow) noexcept {
    return engine().file_stat(std::move(name), follow);
}

future<uint64_t> file_size(sstring name) noexcept {
    return engine().file_size(std::move(name));
}

future<bool> file_accessible(sstring name, access_flags flags) noexcept {
    return engine().file_accessible(std::move(name), flags);
}

future<bool> file_exists(sstring name) noexcept {
    return engine().file_exists(std::move(name));
}

future<> link_file(sstring oldpath, sstring newpath, allow_same allow_same) noexcept {
    if (!allow_same) {
        return engine().link_file(std::move(oldpath), std::move(newpath));
    }
    return engine().link_file(oldpath, newpath)
            .handle_exception_type([oldpath = std::move(oldpath), newpath = std::move(newpath)] (std::system_error& e) mutable {
        int error = e.code().value();
        future<int> process_error = (error == EEXIST ?
                same_file(oldpath, newpath).then([error] (bool same) {
                    return make_ready_future<int>(same ? 0 : error);
                }) :
                make_ready_future<int>(error));

        return process_error.then([oldpath = std::move(oldpath), newpath = std::move(newpath)] (int error) mutable {
            if (error) {
                return make_exception_future<>(make_filesystem_error("link failed", fs::path(oldpath), fs::path(newpath), error));
            } else {
                return make_ready_future<>();
            }
        });
    });
}

future<> chmod(sstring name, file_permissions permissions) noexcept {
    return engine().chmod(std::move(name), permissions);
}

static future<> do_recursive_remove_directory(const fs::path path) noexcept {
    struct work_entry {
        const fs::path path;
        bool listed;

        work_entry(const fs::path path, bool listed)
                : path(std::move(path))
                , listed(listed)
        {
        }
    };

    return do_with(std::deque<work_entry>(), [path = std::move(path)] (auto& work_queue) mutable {
        work_queue.emplace_back(std::move(path), false);
        return do_until([&work_queue] { return work_queue.empty(); }, [&work_queue] () mutable {
            auto ent = work_queue.back();
            work_queue.pop_back();
            if (ent.listed) {
                return remove_file(ent.path.native());
            } else {
                work_queue.emplace_back(ent.path, true);
                return do_with(std::move(ent.path), [&work_queue] (const fs::path& path) {
                    return open_directory(path.native()).then([&path, &work_queue] (file dir) mutable {
                        return do_with(std::move(dir), [&path, &work_queue] (file& dir) mutable {
                            return dir.list_directory([&path, &work_queue] (directory_entry de) mutable {
                                const fs::path sub_path = path / de.name.c_str();
                                if (de.type && *de.type == directory_entry_type::directory) {
                                    work_queue.emplace_back(std::move(sub_path), false);
                                } else {
                                    work_queue.emplace_back(std::move(sub_path), true);
                                }
                                return make_ready_future<>();
                            }).done().then([&dir] () mutable {
                                return dir.close();
                            });
                        });
                    });
                });
            }
        });
    });
}

future<> recursive_remove_directory(fs::path path) noexcept {
    return open_directory((path / "..").native()).then([path = std::move(path)] (file parent) mutable {
        return do_with(std::move(parent), [path = std::move(path)] (file& parent) mutable {
            return do_recursive_remove_directory(std::move(path)).then([&parent] {
                return parent.flush().then([&parent] () mutable {
                    return parent.close();
                });
            });
        });
    });
}

static bool is_same_file(const stat_data& sd1, const stat_data& sd2) {
    return sd1.device_id == sd2.device_id && sd1.inode_number == sd2.inode_number;
}

static auto stat_files(sstring path1, sstring path2) {
    return when_all_succeed(
            [p = std::move(path1)] () mutable {
                return file_stat(std::move(p), follow_symlink::no);
            },
            [p = std::move(path2)] () mutable {
                return file_stat(std::move(p), follow_symlink::no);
            });
}

future<bool> same_file(sstring path1, sstring path2) noexcept {
    return stat_files(std::move(path1), std::move(path2)).then([] (stat_data sd1, stat_data sd2) {
        return make_ready_future<bool>(is_same_file(sd1, sd2));
    });
}

future<> rename_file_ext(sstring oldpath, sstring newpath, allow_overwrite flag) noexcept {
    try {
        return stat_files(oldpath, newpath)
                .then([oldpath = std::move(oldpath), newpath = std::move(newpath), flag] (stat_data sd1, stat_data sd2) {
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
                           (flag == allow_overwrite::if_same && !same)) {
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
    } catch (...) {
        return make_exception_future<>(std::current_exception());
    }
}

} //namespace seastar
