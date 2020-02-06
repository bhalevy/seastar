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
future<bool> same_file(sstring path1, sstring path2, follow_symlink fs);

inline future<bool> same_file(sstring path1, sstring path2) {
    return same_file(std::move(path1), std::move(path2), follow_symlink::yes);
}

} // namespace seastar
