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
#include <seastar/core/file.hh>
#include <seastar/util/std-compat.hh>

namespace seastar {

enum class allow_overwrite {
    never,
    always,
    if_same,
    if_not_same,
};

/// Recursively removes a directory and all of its contents.
///
/// \param path path of the directory to recursively remove
///
/// \note
/// This function fsyncs each component created, and is therefore guaranteed to be stable on disk.
future<> recursive_remove_directory(compat::filesystem::path path);

/// Check if two path names refer to the same file on storage
///
/// \param path1, path2 names to check.
/// \param follow_symlink follow symbolic links.
///
future<bool> same_file(sstring path1, sstring path2, follow_symlink fs = follow_symlink::no);

/// Creates a hard link for a file with extended overwrite semantics.
///
/// \param oldpath existing file name
/// \param newpath name of link
/// \param allow_overwrite determine whether newpath is overwritten if it exists.
///                        never - never overwrite newpath
///                        always - allow overwriting newpath (see details below).
///                        if_same - allow "overwriting" newpath if it is linked to the same file as oldpath.
///                                  link_file_ext does nothing in this case and just results in success,
///                                  in contrast to link_file (see link(2)).
///                        if_not_same - allow overwriting newpath only if it is not linked to the same file as oldpath
///                                      (see details below).
///
/// \note
/// directories are not allowed to be hard linked.
/// When overwriting newpath is allowed, it must not be a directory either.
/// It is first removed and the operation is retried one more time.
/// Therefore, link_file_ext is guaranteed to be atomic only for:
///     allow_overwrite::never and allow_overwrite::if_same.
///
/// The links are not guaranteed to be stable on disk, unless the
/// containing directories are sync'ed.
///
future<> link_file_ext(sstring oldpath, sstring newpath, allow_overwrite flag);

/// Renames (moves) a file with extended overwrite semantics.
///
/// \param oldpath existing file name
/// \param newpath new file name
/// \param allow_overwrite determine whether new_name is overwritten if it exists.
///                        never - never overwrite newpath
///                        always - allow overwriting newpath (see details below).
///                        if_same - allow "overwriting" newpath if it is linked to the same file as oldpath.
///                                  link_file_ext just removes oldpath in this case, in contrast to
///                                  rename_file that has no effect in this case (see rename(2)).
///                        if_not_same - allow overwriting newpath only if it is not linked to the same file as oldpath.
///                                      if they are the same, an (EEXIST) error is returned, in contrast to
///                                      rename_file that has no effect in this case (see rename(2)).
///
/// \note
///
/// oldpath can specify a directory. In this case, newpath must either not exist, or it must specify an empty directory.
/// If oldpath is not a directory, newpath must not be a directory either.
///
/// If oldpath refers to a symbolic link, the link is renamed; if newpath refers to a symbolic link,
/// the link may be overwritten, based on allow_overwrite.
///
/// When overwriting newpath, it is first removed and the operation is retried one more time.
/// Therefore, rename_file_ext is not guaranteed to be atomic unless it is passed the allow_overwrite::never flag.
///
/// rename_file_ext's handles the case when old_name new_name are linked to the same file
/// differently than rename_file and the rename(2) system call. In this case, rename_file_ext
/// removes oldpath with allow_overwrite::{always,if_same} or returns an error with allow_overwrite::if_not_same;
/// while rename_file returns success but has no effect, keeping both oldpath and newpath in place.
///
/// The rename is not guaranteed to be stable on disk, unless the
/// containing directories are sync'ed.
///
future<> rename_file_ext(sstring oldpath, sstring newpath, allow_overwrite);

} // namespace seastar
