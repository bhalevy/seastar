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
#include <seastar/util/std-compat.hh>

namespace seastar {

/// Recursively removes a directory and all of its contents.
///
/// \param path path of the directory to recursively remove
///
/// \note
/// Unlike `rm -rf` path has to be a directory and may not refer to a regular file.
///
/// The function flushes the parent directory of the removed path and so guaranteeing that
/// the remove is stable on disk.
///
/// The function bails out on first error. In that case, some files and/or sub-directories
/// (and their contents) may be left behind at the level in which the error was detected.
///
future<> recursive_remove_directory(compat::filesystem::path path);

/// Check if two path names refer to the same file on storage
///
/// \param path1, path2 names to check.
/// \param follow_symlink follow symbolic links.
///
future<bool> same_file(sstring path1, sstring path2, follow_symlink fs = follow_symlink::no);

struct allow_same_tag { };
using allow_same = bool_class<allow_same_tag>;

/// Creates a hard link for a file with extended overwrite semantics.
///
/// \param oldpath existing file name
/// \param newpath name of link
/// \param allow_same allow the function to succeed if oldpath and newpath are already linked to the same file.
///
/// \note
/// directories are not allowed to be hard linked.
///
/// The links are not guaranteed to be stable on disk, unless the
/// containing directories are sync'ed.
///
future<> link_file_ext(sstring oldpath, sstring newpath, allow_same allow_same);

} // namespace seastar
