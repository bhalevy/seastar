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
 * Copyright (C) 2018 ScyllaDB
 */

#pragma once

#include <optional>
#include <string_view>
#include <variant>

#include <filesystem>

#include <memory_resource>


// Defining SEASTAR_ASAN_ENABLED in here is a bit of a hack, but
// convenient since it is build system independent and in practice
// everything includes this header.

#ifndef __has_feature
#define __has_feature(x) 0
#endif

// This allows us to work around
// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=95368 while keeping
// track of which cases can be dropped once gcc is fixed.
//
// Background info:
//
// An oddity of c++ is that if a const value is captured by copy in a
// lambda, the corresponding member is const. For example, in
// auto f(const foo& bar) {
//     return [bar]() {};
// }
// The produced lambda has a type that looks like
// struct anonymous {
//     const foo bar;
//     void operator()() const {}
// };
// Note that making the lambda mutable would only change the
// operator() to not be const, but the member variable would still be
// const.
// This oddity causes problems in seastar because we need to move
// function objects in contexts where it is impossible to handle
// exceptions. The const member variable means that an attempt to move
// the function object will actually copy it, which is more likely to
// be a throwing operation.
//
// The easiest way to avoid this problem is to write the lambda
// capture as "bar = bar". This produces a copy as before, but the
// produced function object now has a member of type "foo" instead of
// "const foo", so it can be moved.
//
// Note that that the initial construction of the lambda still
// involves a copy, so changing
//   func().finally([bar] {});
// to
//   func().finally([bar = bar] {});
// Fixes one bug (the function object is now movable) but another
// remains (constructing the lambda in the first place might throw
// after func() constructed a future).
// To fix both issues, change the code to
//   foo zed = bar;
//   func().finally([bar = std::move(zed)] {});
// or
//   auto f = [bar = bar] {};
//   func().finally(std::move(f));
//
// The bug:
//
// In addition to the this, the gcc bug mentioned above causes a
// lambda capture in a nested lambda to also have a const type. To be
// able to work around that bug without losing track of when we are
// doing so, we added this macro which expands to the above workaround
// with gcc but the original expression with clang.
//
// Note that the above observation about exceptions during creation
// still applies when using this macro.
#ifdef __clang__
#define SEASTAR_GCC_BZ95368(x) x
#else
#define SEASTAR_GCC_BZ95368(x) x = x
#endif

// clang uses __has_feature, gcc defines __SANITIZE_ADDRESS__
#if __has_feature(address_sanitizer) || defined(__SANITIZE_ADDRESS__)
#define SEASTAR_ASAN_ENABLED
#endif
