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
 * Copyright (C) 2021 Cloudius Systems, Ltd.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/util/concepts.hh>

namespace seastar {

SEASTAR_CONCEPT(
template <typename Object>
concept closeable = requires (Object o) {
    { o.close() } -> std::same_as<future<>>;
};
)

/// Template helper to close \c obj when destroyed.
///
/// \tparam Object a class exposing a \c close() method that returns a \c future<>
///         that is called when the controller is destroyed.
template <typename Object>
SEASTAR_CONCEPT( requires closeable<Object> )
class close_controller {
    Object& _obj;
    bool _closed = false;

    void do_close() noexcept {
        if (!_closed) {
            _closed = true;
            _obj.close().get();
        }
    }
public:
    close_controller(Object& obj) noexcept : _obj(obj) {}
    ~close_controller() {
        do_close();
    }
    /// Close \c obj once now.
    void close_now() noexcept {
        assert(!_closed);
        do_close();
    }
};

/// Auto-close an object.
///
/// \param obj object to auto-close.
///
/// \return A deferred action that closes \c obj.
///
/// \note Can be used only in a seatstar thread.
template <typename Object>
SEASTAR_CONCEPT( requires closeable<Object> )
inline auto deferred_close(Object& obj) {
    return close_controller(obj);
}

SEASTAR_CONCEPT(
template <typename Object>
concept stoppable = requires (Object o) {
    { o.stop() } -> std::same_as<future<>>;
};
)

/// Template helper to stop \c obj when destroyed.
///
/// \tparam Object a class exposing a \c stop() method that returns a \c future<>
///         that is called when the controller is destroyed.
template <typename Object>
SEASTAR_CONCEPT( requires stoppable<Object> )
class stop_controller {
    Object& _obj;
    bool _stopped = false;

    void do_stop() noexcept {
        if (!_stopped) {
            _stopped = true;
            _obj.stop().get();
        }
    }
public:
    stop_controller(Object& obj) noexcept : _obj(obj) {}
    ~stop_controller() {
        do_stop();
    }
    /// Stop \c obj once now.
    void stop_now() noexcept {
        assert(!_stopped);
        do_stop();
    }
};

/// Auto-stop an object.
///
/// \param obj object to auto-stop.
///
/// \return A deferred action that stops \c obj.
///
/// \note Can be used only in a seatstar thread.
template <typename Object>
SEASTAR_CONCEPT( requires stoppable<Object> )
inline auto deferred_stop(Object& obj) {
    return stop_controller(obj);
}

} // namespace seastar
