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
 * Copyright (C) 2017 ScyllaDB.
 */

#pragma once

#include <seastar/util/noncopyable_function.hh>
#include <seastar/util/optimized_optional.hh>
#include <seastar/util/std-compat.hh>

#include <boost/intrusive/list.hpp>

#include <exception>

namespace bi = boost::intrusive;

namespace seastar {

/// \addtogroup fiber-module
/// @{

/// Exception thrown when an \ref abort_source object has been
/// notified by the \ref abort_source::request_abort() method.
class abort_requested_exception : public std::exception {
public:
    virtual const char* what() const noexcept override {
        return "abort requested";
    }
};

/// Facility to communicate a cancellation request to a fiber.
/// Callbacks can be registered with the \c abort_source, which are called
/// atomically with a call to request_abort().
class abort_source {
    using subscription_callback_type = noncopyable_function<void (const std::optional<std::exception_ptr>&) noexcept>;
    using naive_subscription_callback_type = noncopyable_function<void() noexcept>;

public:
    /// Represents a handle to the callback registered by a given fiber. Ending the
    /// lifetime of the \c subscription will unregister the callback, if it hasn't
    /// been invoked yet.
    class subscription : public bi::list_base_hook<bi::link_mode<bi::auto_unlink>> {
        friend class abort_source;

        subscription_callback_type _target;

        explicit subscription(abort_source& as, subscription_callback_type target)
                : _target(std::move(target)) {
            as._subscriptions->push_back(*this);
        }

        struct naive_cb_tag {}; // to disambiguate constructors
        explicit subscription(naive_cb_tag, abort_source& as, naive_subscription_callback_type naive_cb)
                : _target([cb = std::move(naive_cb)] (const std::optional<std::exception_ptr>&) noexcept { cb(); }) {
            as._subscriptions->push_back(*this);
        }

        void on_abort(const std::optional<std::exception_ptr>& ex) noexcept {
            _target(ex);
        }

    public:
        subscription() = default;

        subscription(subscription&& other) noexcept(std::is_nothrow_move_constructible<subscription_callback_type>::value)
                : _target(std::move(other._target)) {
            subscription_list_type::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
        }

        subscription& operator=(subscription&& other) noexcept(std::is_nothrow_move_assignable<subscription_callback_type>::value) {
            if (this != &other) {
                _target = std::move(other._target);
                unlink();
                subscription_list_type::node_algorithms::swap_nodes(other.this_ptr(), this_ptr());
            }
            return *this;
        }

        explicit operator bool() const noexcept {
            return is_linked();
        }
    };

private:
    using subscription_list_type = bi::list<subscription, bi::constant_time_size<false>>;
    std::optional<subscription_list_type> _subscriptions = subscription_list_type();
    std::exception_ptr _ex;

    void do_request_abort(std::optional<std::exception_ptr> ex) noexcept {
        assert(_subscriptions);
        _ex = ex.value_or(get_default_exception());
        auto subs = std::exchange(_subscriptions, std::nullopt);
        while (!subs->empty()) {
            subscription& s = subs->front();
            s.unlink();
            s.on_abort(ex);
        }
    }

    friend class abort_when_either;
    friend class abort_when_both;
public:
    abort_source() = default;
    virtual ~abort_source() = default;

    abort_source(abort_source&&) = default;
    abort_source& operator=(abort_source&&) = default;

    /// Delays the invocation of the callback \c f until \ref request_abort() is called.
    /// \returns an engaged \ref optimized_optional containing a \ref subscription that can be used to control
    ///          the lifetime of the callback \c f, if \ref abort_requested() is \c false. Otherwise,
    ///          returns a disengaged \ref optimized_optional.
    template <typename Func>
    SEASTAR_CONCEPT(requires
            requires (Func f, const std::optional<std::exception_ptr>& opt_ex) { { f(opt_ex) } noexcept -> std::same_as<void>; }
        ||  requires (Func f) { { f() } noexcept -> std::same_as<void>; }
    )
    [[nodiscard]]
    optimized_optional<subscription> subscribe(Func&& f) {
        if (abort_requested()) {
            return { };
        }
        if constexpr (std::is_invocable_v<Func, std::exception_ptr>) {
            return { subscription(*this, std::forward<Func>(f)) };
        } else {
            return { subscription(subscription::naive_cb_tag{}, *this, std::forward<Func>(f)) };
        }
    }

    /// Requests that the target operation be aborted. Current subscriptions
    /// are invoked inline with this call with a disengaged optional<std::exception_ptr>,
    /// and no new ones can be registered.
    /// Must be called exactly once, otherwise the program will be aborted.
    void request_abort() noexcept {
        do_request_abort(std::nullopt);
    }

    /// Requests that the target operation be aborted with a given \c exception_ptr.
    /// Current subscriptions are invoked inline with this exception,
    /// and no new ones can be registered.
    /// Must be called exactly once, otherwise the program will be aborted.
    void request_abort_ex(std::exception_ptr ex) noexcept {
        do_request_abort(std::make_optional(std::move(ex)));
    }

    /// Requests that the target operation be aborted with a given \c Exception object.
    /// Current subscriptions are invoked inline with this exception, converted to std::exception_ptr,
    /// and no new ones can be registered.
    /// Must be called exactly once, otherwise the program will be aborted.
    template <typename Exception>
    void request_abort_ex(Exception&& e) noexcept {
        do_request_abort(std::make_optional(std::make_exception_ptr(std::forward<Exception>(e))));
    }

    /// Returns whether an abort has been requested.
    bool abort_requested() const noexcept {
        return !_subscriptions;
    }


    /// Throws a \ref abort_requested_exception if cancellation has been requested.
    void check() const {
        if (abort_requested()) {
            std::rethrow_exception(_ex);
        }
    }

    /// Returns the default exception type (\ref abort_requested_exception) for this abort source.
    /// Overridable by derived classes.
    virtual std::exception_ptr get_default_exception() const noexcept {
        return make_exception_ptr(abort_requested_exception());
    }

    /// Returns the exception_ptr abort was requested with
    /// or nullptr if no abort was requested.
    std::exception_ptr get_exception() const noexcept {
        return _ex;
    }
};

class abort_when_either {
    abort_source* _as0 = nullptr;
    abort_source* _as1 = nullptr;
    abort_source _as;
    optimized_optional<seastar::abort_source::subscription> _sub0;
    optimized_optional<seastar::abort_source::subscription> _sub1;

private:
    void do_subscribe() noexcept {
        auto callback = [this] (const std::optional<std::exception_ptr>& opt_ex) noexcept {
            if (!_as.abort_requested()) {
                _as.do_request_abort(std::move(opt_ex));
            }
        };
        _sub0 = _as0->subscribe(callback);
        if (!_sub0) {
            _as.do_request_abort(_as0->get_exception());
        } else {
            _sub1 = _as1->subscribe(callback);
            if (!_sub1) {
                _as.do_request_abort(_as1->get_exception());
                _sub0 = {};
            }
        }
    }
public:
    abort_when_either() = default;
    abort_when_either(abort_source& as0, abort_source& as1) noexcept
        : _as0(&as0)
        , _as1(&as1)
    {
        do_subscribe();
    }
    abort_when_either(abort_when_either&& o) noexcept
        : _as0(std::exchange(o._as0, nullptr))
        , _as1(std::exchange(o._as1, nullptr))
        , _as(std::move(o._as))
    {
        if (!_as.abort_requested()) {
            do_subscribe();
        }
    }
    abort_when_either& operator=(abort_when_either&& o) noexcept {
        if (this != &o) {
            _as0 = std::exchange(o._as0, nullptr);
            _as1 = std::exchange(o._as1, nullptr);
            _as = std::move(o._as);
            if (!_as.abort_requested()) {
                do_subscribe();
            } else {
                _sub0 = {};
                _sub1 = {};
            }
        }
        return *this;
    }

    seastar::abort_source& abort_source() noexcept {
        return _as;
    }
};

class abort_when_both {
    abort_source* _as0 = nullptr;
    abort_source* _as1 = nullptr;
    int _aborted = 0;
    abort_source _as;
    optimized_optional<seastar::abort_source::subscription> _sub0;
    optimized_optional<seastar::abort_source::subscription> _sub1;

private:
    void do_subscribe() noexcept {
        auto callback = [this] (const std::optional<std::exception_ptr>& opt_ex) noexcept {
            if (_aborted++ && !_as.abort_requested()) {
                _as.do_request_abort(std::move(opt_ex));
            }
        };
        _sub0 = _as0->subscribe(callback);
        _sub1 = _as1->subscribe(callback);
        _aborted = !_sub0 + !_sub1;
        if (_aborted == 2) {
            _as.do_request_abort(_as1->get_exception());
        }
    }
public:
    abort_when_both() = default;
    abort_when_both(abort_source& as0, abort_source& as1) noexcept
        : _as0(&as0)
        , _as1(&as1)
    {
        do_subscribe();
    }
    abort_when_both(abort_when_both&& o) noexcept
        : _as0(std::exchange(o._as0, nullptr))
        , _as1(std::exchange(o._as1, nullptr))
        , _aborted(std::exchange(o._aborted, 0))
        , _as(std::move(o._as))
    {
        if (!_as.abort_requested()) {
            do_subscribe();
        }
    }
    abort_when_both& operator=(abort_when_both&& o) noexcept {
        if (this != &o) {
            _as0 = std::exchange(o._as0, nullptr);
            _as1 = std::exchange(o._as1, nullptr);
            _aborted = std::exchange(o._aborted, 0);
            _as = std::move(o._as);
            if (!_as.abort_requested()) {
                do_subscribe();
            } else {
                _sub0 = {};
                _sub1 = {};
            }
        }
        return *this;
    }

    seastar::abort_source& abort_source() noexcept {
        return _as;
    }
};

/// @}

}
