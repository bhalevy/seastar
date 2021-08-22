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
 * Copyright 2021-present ScyllaDB
 */

#pragma once

#include <seastar/core/abort_source.hh>
#include <seastar/core/timer.hh>
#include <seastar/core/timed_out_error.hh>

/// \defgroup timers Timers
///
/// Seastar provides timers that can be defined to run a callback at a certain
/// time point in the future; timers are provided for \ref lowres_clock (10ms
/// resolution, efficient), for std::chrono::steady_clock (accurate but less
/// efficient) and for \ref manual_clock (for testing purposes).
///
/// Timers are optimized for cancellation; that is, adding a timer and cancelling
/// it is very efficient. This means that attaching a timer per object for
/// a timeout that rarely happens is reasonable; one does not have to maintain
/// a single timer and a sorted list for this use case.
///
/// Timer callbacks should be short and execute quickly. If involved processing
/// is required, a timer can launch a continuation.

namespace seastar {

namespace util {

/// An \ref abort_source \ref timer that requests abort with \ref timed_out_error
/// when a given \c timeout expires.
template <typename Clock = steady_clock_type>
class timeout_abort_source : public abort_source, public timer<Clock> {
public:
    typedef typename Clock::duration duration;
    typedef typename Clock::time_point time_point;

    timeout_abort_source() = default;

    timeout_abort_source(time_point timeout) {
        if (timeout != time_point::max()) {
            this->set_callback([this] { this->request_abort_ex(std::make_exception_ptr(timed_out_error())); });
            this->arm(timeout);
        }
    }

    timeout_abort_source(duration delta)
        : timeout_abort_source(now() + delta)
    { }

    static time_point now() noexcept {
        return Clock::now();
    }
};

} // namespace util

} // namespace seastar
