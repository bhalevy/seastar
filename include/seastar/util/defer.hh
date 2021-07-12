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
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */

#pragma once

#include <type_traits>
#include <utility>

#include <seastar/util/noncopyable_function.hh>

#ifdef SEASTAR_DEFERRED_ACTION_NOEXCEPT
#define DEFERRED_ACTION_NOEXCEPT noexcept
#else
#define DEFERRED_ACTION_NOEXCEPT
#endif

namespace seastar {

class deferred_action {
public:
    using function_type = noncopyable_function<void() DEFERRED_ACTION_NOEXCEPT>;
private:
    function_type _func;
    bool _cancelled = false;
public:
    static_assert(std::is_nothrow_move_constructible_v<function_type>, "Func(Func&&) must be noexcept");
    deferred_action(function_type&& func) noexcept : _func(std::move(func)) {}
    deferred_action(deferred_action&& o) noexcept : _func(std::move(o._func)), _cancelled(o._cancelled) {
        o._cancelled = true;
    }
    deferred_action& operator=(deferred_action&& o) noexcept {
        if (this != &o) {
            this->~deferred_action();
            new (this) deferred_action(std::move(o));
        }
        return *this;
    }
    deferred_action(const deferred_action&) = delete;
    ~deferred_action() { if (!_cancelled) { _func(); }; }
    void cancel() { _cancelled = true; }
};

inline
deferred_action
defer(deferred_action::function_type&& func) {
    return deferred_action(std::move(func));
}

}
