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
 * Copyright (C) 2020 ScyllaDB.
 */

#pragma once

#include <seastar/core/future.hh>
#include <seastar/core/task.hh>
#include <seastar/util/bool_class.hh>

namespace seastar {

/// \addtogroup future-util
/// @{

// The AsyncAction concept represents an action which can complete later than
// the actual function invocation. It is represented by a function which
// returns a future which resolves when the action is done.

struct stop_iteration_tag { };
using stop_iteration = bool_class<stop_iteration_tag>;

namespace internal {

template <typename AsyncAction>
class repeater final : public continuation_base<stop_iteration> {
    promise<> _promise;
    AsyncAction _action;
public:
    explicit repeater(AsyncAction&& action) : _action(std::move(action)) {}
    repeater(stop_iteration si, AsyncAction&& action) : repeater(std::move(action)) {
        _state.set(si);
    }
    future<> get_future() { return _promise.get_future(); }
    task* waiting_task() noexcept override { return _promise.waiting_task(); }
    virtual void run_and_dispose() noexcept override {
        if (_state.failed()) {
            _promise.set_exception(std::move(_state).get_exception());
            delete this;
            return;
        } else {
            if (std::get<0>(_state.get()) == stop_iteration::yes) {
                _promise.set_value();
                delete this;
                return;
            }
            _state = {};
        }
        try {
            do {
                auto f = futurize_invoke(_action);
                if (!f.available()) {
                    internal::set_callback(f, this);
                    return;
                }
                if (f.get0() == stop_iteration::yes) {
                    _promise.set_value();
                    delete this;
                    return;
                }
            } while (!need_preempt());
        } catch (...) {
            _promise.set_exception(std::current_exception());
            delete this;
            return;
        }
        _state.set(stop_iteration::no);
        schedule(this);
    }
};

} // namespace internal

// Delete these overloads so that the actual implementation can use a
// universal reference but still reject lvalue references.
template<typename AsyncAction>
future<> repeat(const AsyncAction& action) noexcept = delete;
template<typename AsyncAction>
future<> repeat(AsyncAction& action) noexcept = delete;

/// Invokes given action until it fails or the function requests iteration to stop by returning
/// \c stop_iteration::yes.
///
/// \param action a callable taking no arguments, returning a future<stop_iteration>.  Will
///               be called again as soon as the future resolves, unless the
///               future fails, action throws, or it resolves with \c stop_iteration::yes.
///               If \c action is an r-value it can be moved in the middle of iteration.
/// \return a ready future if we stopped successfully, or a failed future if
///         a call to to \c action failed.
template<typename AsyncAction>
SEASTAR_CONCEPT( requires seastar::InvokeReturns<AsyncAction, stop_iteration> || seastar::InvokeReturns<AsyncAction, future<stop_iteration>> )
inline
future<> repeat(AsyncAction&& action) noexcept {
    using futurator = futurize<std::result_of_t<AsyncAction()>>;
    static_assert(std::is_same<future<stop_iteration>, typename futurator::type>::value, "bad AsyncAction signature");
    try {
        do {
            // Do not type-erase here in case this is a short repeat()
            auto f = futurator::invoke(action);

            if (!f.available()) {
              return [&] () noexcept {
                memory::disable_failure_guard dfg;
                auto repeater = new internal::repeater<AsyncAction>(std::move(action));
                auto ret = repeater->get_future();
                internal::set_callback(f, repeater);
                return ret;
              }();
            }

            if (f.get0() == stop_iteration::yes) {
                return make_ready_future<>();
            }
        } while (!need_preempt());

        auto repeater = new internal::repeater<AsyncAction>(stop_iteration::no, std::move(action));
        auto ret = repeater->get_future();
        schedule(repeater);
        return ret;
    } catch (...) {
        return make_exception_future(std::current_exception());
    }
}

/// \cond internal

template <typename T>
struct repeat_until_value_type_helper;

/// Type helper for repeat_until_value()
template <typename T>
struct repeat_until_value_type_helper<future<std::optional<T>>> {
    /// The type of the value we are computing
    using value_type = T;
    /// Type used by \c AsyncAction while looping
    using optional_type = std::optional<T>;
    /// Return type of repeat_until_value()
    using future_type = future<value_type>;
};

/// Return value of repeat_until_value()
template <typename AsyncAction>
using repeat_until_value_return_type
        = typename repeat_until_value_type_helper<typename futurize<std::result_of_t<AsyncAction()>>::type>::future_type;

/// \endcond

namespace internal {

template <typename AsyncAction, typename T>
class repeat_until_value_state final : public continuation_base<std::optional<T>> {
    promise<T> _promise;
    AsyncAction _action;
public:
    explicit repeat_until_value_state(AsyncAction action) : _action(std::move(action)) {}
    repeat_until_value_state(std::optional<T> st, AsyncAction action) : repeat_until_value_state(std::move(action)) {
        this->_state.set(std::make_tuple(std::move(st)));
    }
    future<T> get_future() { return _promise.get_future(); }
    task* waiting_task() noexcept override { return _promise.waiting_task(); }
    virtual void run_and_dispose() noexcept override {
        if (this->_state.failed()) {
            _promise.set_exception(std::move(this->_state).get_exception());
            delete this;
            return;
        } else {
            auto v = std::get<0>(std::move(this->_state).get());
            if (v) {
                _promise.set_value(std::move(*v));
                delete this;
                return;
            }
            this->_state = {};
        }
        try {
            do {
                auto f = futurize_invoke(_action);
                if (!f.available()) {
                    internal::set_callback(f, this);
                    return;
                }
                auto ret = f.get0();
                if (ret) {
                    _promise.set_value(std::make_tuple(std::move(*ret)));
                    delete this;
                    return;
                }
            } while (!need_preempt());
        } catch (...) {
            _promise.set_exception(std::current_exception());
            delete this;
            return;
        }
        this->_state.set(std::nullopt);
        schedule(this);
    }
};

} // namespace internal

/// Invokes given action until it fails or the function requests iteration to stop by returning
/// an engaged \c future<std::optional<T>> or std::optional<T>.  The value is extracted
/// from the \c optional, and returned, as a future, from repeat_until_value().
///
/// \param action a callable taking no arguments, returning a future<std::optional<T>>
///               or std::optional<T>.  Will be called again as soon as the future
///               resolves, unless the future fails, action throws, or it resolves with
///               an engaged \c optional.  If \c action is an r-value it can be moved
///               in the middle of iteration.
/// \return a ready future if we stopped successfully, or a failed future if
///         a call to to \c action failed.  The \c optional's value is returned.
template<typename AsyncAction>
SEASTAR_CONCEPT( requires requires (AsyncAction aa) {
    bool(futurize_invoke(aa).get0());
    futurize_invoke(aa).get0().value();
} )
repeat_until_value_return_type<AsyncAction>
repeat_until_value(AsyncAction action) noexcept {
    using futurator = futurize<std::result_of_t<AsyncAction()>>;
    using type_helper = repeat_until_value_type_helper<typename futurator::type>;
    // the "T" in the documentation
    using value_type = typename type_helper::value_type;
    using optional_type = typename type_helper::optional_type;
    do {
        auto f = futurator::invoke(action);

        if (!f.available()) {
          return [&] () noexcept {
            memory::disable_failure_guard dfg;
            auto state = new internal::repeat_until_value_state<AsyncAction, value_type>(std::move(action));
            auto ret = state->get_future();
            internal::set_callback(f, state);
            return ret;
          }();
        }

        if (f.failed()) {
            return make_exception_future<value_type>(f.get_exception());
        }

        optional_type&& optional = std::move(f).get0();
        if (optional) {
            return make_ready_future<value_type>(std::move(optional.value()));
        }
    } while (!need_preempt());

    try {
        auto state = new internal::repeat_until_value_state<AsyncAction, value_type>(std::nullopt, std::move(action));
        auto f = state->get_future();
        schedule(state);
        return f;
    } catch (...) {
        return make_exception_future<value_type>(std::current_exception());
    }
}

namespace internal {

template <typename StopCondition, typename AsyncAction>
class do_until_state final : public continuation_base<> {
    promise<> _promise;
    StopCondition _stop;
    AsyncAction _action;
public:
    explicit do_until_state(StopCondition stop, AsyncAction action) : _stop(std::move(stop)), _action(std::move(action)) {}
    future<> get_future() { return _promise.get_future(); }
    task* waiting_task() noexcept override { return _promise.waiting_task(); }
    virtual void run_and_dispose() noexcept override {
        if (_state.available()) {
            if (_state.failed()) {
                _promise.set_urgent_state(std::move(_state));
                delete this;
                return;
            }
            _state = {}; // allow next cycle to overrun state
        }
        try {
            do {
                if (_stop()) {
                    _promise.set_value();
                    delete this;
                    return;
                }
                auto f = _action();
                if (!f.available()) {
                    internal::set_callback(f, this);
                    return;
                }
                if (f.failed()) {
                    f.forward_to(std::move(_promise));
                    delete this;
                    return;
                }
            } while (!need_preempt());
        } catch (...) {
            _promise.set_exception(std::current_exception());
            delete this;
            return;
        }
        schedule(this);
    }
};

} // namespace internal

/// Invokes given action until it fails or given condition evaluates to true.
///
/// \param stop_cond a callable taking no arguments, returning a boolean that
///                  evalutes to true when you don't want to call \c action
///                  any longer
/// \param action a callable taking no arguments, returning a future<>.  Will
///               be called again as soon as the future resolves, unless the
///               future fails, or \c stop_cond returns \c true.
/// \return a ready future if we stopped successfully, or a failed future if
///         a call to to \c action failed.
template<typename AsyncAction, typename StopCondition>
SEASTAR_CONCEPT( requires seastar::InvokeReturns<StopCondition, bool> && seastar::InvokeReturns<AsyncAction, future<>> )
inline
future<> do_until(StopCondition stop_cond, AsyncAction action) noexcept {
    using namespace internal;
    for (;;) {
        if (stop_cond()) {
            return make_ready_future<>();
        }
        auto f = futurize_invoke(action);
        if (f.failed()) {
            return f;
        }
        if (!f.available() || need_preempt()) {
            return [&] () noexcept {
                memory::disable_failure_guard dfg;
                auto task = new do_until_state<StopCondition, AsyncAction>(std::move(stop_cond), std::move(action));
                auto ret = task->get_future();
                internal::set_callback(f, task);
                return ret;
            }();
        }
    }
}

/// Invoke given action until it fails.
///
/// Calls \c action repeatedly until it returns a failed future.
///
/// \param action a callable taking no arguments, returning a \c future<>
///        that becomes ready when you wish it to be called again.
/// \return a future<> that will resolve to the first failure of \c action
template<typename AsyncAction>
SEASTAR_CONCEPT( requires seastar::InvokeReturns<AsyncAction, future<>> )
inline
future<> keep_doing(AsyncAction action) noexcept {
    return repeat([action = std::move(action)] () mutable {
        return action().then([] {
            return stop_iteration::no;
        });
    });
}

/// @}

} // namespace seastar
