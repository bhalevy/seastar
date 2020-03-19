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
 * Copyright (C) 2020 ScyllaDB
 */

#pragma once

#include <iterator>

namespace seastar {
namespace internal {

// FIXME:
// Define a reverse iterator since std::reverse_iterator is not
// nothrow move constructible as required by do_for_each() noexcept.
// We can't simply call do_for_each(x.rbegin(), x.rend(), [] {});
// Instead, this reverse_iterator is used internally by
// do_for_each_reverse(x.begin(), x.end(), [] {});
//
// See https://gcc.gnu.org/bugzilla/show_bug.cgi?id=94418
template<typename Iterator>
class reverse_iterator : public std::reverse_iterator<Iterator> {

    static_assert(std::is_nothrow_move_constructible<Iterator>::value,
            "Iterator's move constructor must not throw");

public:
    explicit reverse_iterator(Iterator base) noexcept
        : std::reverse_iterator<Iterator>(std::move(base)) { }

    reverse_iterator(reverse_iterator&& o) noexcept
        : std::reverse_iterator<Iterator>(std::move(o)) { }
};

} // namespace internal
} // namespace seastar
