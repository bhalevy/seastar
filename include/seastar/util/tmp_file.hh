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

/// Returns a valid filename known to not exist at the time the function runs.
///
/// \param path_template - path where the file is to be created, optionally including
///                        a template for the file name.
///
/// \note
///    The given path must exist and be writable.
///    A string of 2 or more XX's in path_template will be replaced in the resulting name by a unique string,
///    and if none found, default_tmp_name_template is used.
///
future<compat::filesystem::path> tmp_name(const compat::filesystem::path path_template = default_tmp_name_template);

} // namespace seastar
