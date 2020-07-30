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

#include <seastar/util/alloc_failure_injector.hh>

#include <type_traits>
#include <utility>

namespace seastar {

template <typename Func>
class deferred_action {
    Func _func;
    bool _cancelled = false;
public:
    static_assert(std::is_nothrow_move_constructible<Func>::value, "Func(Func&&) must be noexcept");
    deferred_action(Func&& func) noexcept : _func(std::move(func)) {}
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
    ~deferred_action() {
        // A destructor is noexcept, but we still want defer() to be
        // usable in places where we can't handle a std::bad_alloc
        // anyway.
        memory::disable_failure_guard dfg;
        if (!_cancelled) {
            _func();
        }
    }
    void cancel() { _cancelled = true; }
};

template <typename Func>
inline
deferred_action<Func>
defer(Func&& func) {
    return deferred_action<Func>(std::forward<Func>(func));
}

}
