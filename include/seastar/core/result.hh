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
 * Copyright (C) 2022-present ScyllaDB
 */

#pragma once

#include <stdexcept>
#include <cassert>
#include <utility>
#include <optional>

#include <seastar/util/concepts.hh>

namespace seastar {

namespace internal {

class result_base {
public:
    enum class state {
        init,
        has_value,
        failed,
        consumed
    };

private:
    state _state = state::init;
    std::exception_ptr _ex = nullptr;

public:
    explicit result_base() = default;

    struct with_value_tag {};
    explicit result_base(with_value_tag) noexcept
        : _state(state::has_value)
    {}
    explicit result_base(std::exception_ptr ex) noexcept
        : _state(state::failed)
        , _ex(std::move(ex))
    {};
    result_base(result_base&& o) noexcept
        : _state(std::exchange(o._state, state::consumed))
        , _ex(std::move(o._ex))
    {
        assert(_state != state::consumed);
    }

    ~result_base();

    result_base& operator=(result_base&& o) {
        assert(o._state != state::consumed);
        if (this != &o) {
            _state = std::exchange(o._state, state::consumed);
            _ex = std::move(o._ex);
        }
        return *this;
    }

    const bool has_value() const noexcept { return _state == state::has_value; }
    const bool failed() const noexcept { return bool(_ex); }
    const bool available() const noexcept { return has_value() || failed() ; }

    void get() {
        auto old_state = std::exchange(_state, state::consumed);
        switch (old_state) {
        case state::has_value:
            return;
        case state::failed:
            std::rethrow_exception(std::move(_ex));
            break;
        default:
            throw std::runtime_error("result not available");
        }
    }

    void set_value() {
        if (_state != state::init) {
            throw std::runtime_error("cannot set result value");
        }
        _state = state::has_value;
    }

    std::exception_ptr get_exception() noexcept {
        assert(_state == state::failed);
        _state = state::consumed;
        return std::move(_ex);
    }

    void set_exception(std::exception_ptr ex) {
        if (_state != state::init) {
            throw std::runtime_error("cannot set result exception");
        }
        _state = state::failed;
        _ex = std::move(ex);
    }
};

} // namespace internal

template <class T = void>
class result;

template<>
class result<void> final : public internal::result_base {
public:
    explicit result() = default;
    explicit result(internal::result_base::with_value_tag t) noexcept
        : internal::result_base(t)
    {}
    explicit result(std::exception_ptr ex) noexcept
        : internal::result_base(std::move(ex))
    {}
    result(result&&) = default;

    result& operator=(result&& o) {
        if (this != &o) {
            this->internal::result_base::operator=(std::move(*dynamic_cast<internal::result_base*>(&o)));
        }
        return *this;
    }

    void set_value() {
        internal::result_base::set_value();
    }

    template <typename Func, typename... Args>
    SEASTAR_CONCEPT(requires std::same_as<std::invoke_result_t<Func, Args...>, void>)
    static auto invoke(Func&& func, Args&&... args) noexcept {
        try {
            func(std::forward<Args>(args)...);
            return result(internal::result_base::with_value_tag{});
        } catch (...) {
            return result(std::current_exception());
        }
    }
};

template <class T>
class result final : public internal::result_base {
    std::optional<T> _value = std::nullopt;
public:
    result() = default;
    explicit result(T value)
        : internal::result_base()
        , _value(std::move(value))
    {
        internal::result_base::set_value();
    }
    explicit result(std::exception_ptr ex) noexcept
        : internal::result_base(std::move(ex))
    {}
    result(result&&) = default;

    result& operator=(result&& o) {
        if (this != &o) {
            this->internal::result_base::operator=(std::move(*dynamic_cast<internal::result_base*>(&o)));
            _value = std::move(o._value);
        }
        return *this;
    }

    const T& get() const {
        internal::result_base::get();
        return *_value;
    }
    T& get() {
        internal::result_base::get();
        return *_value;
    }

    void set_value(T&& value) {
        internal::result_base::set_value();
        _value = std::move(value);
    }

    template <typename Func, typename... Args>
    SEASTAR_CONCEPT(requires std::same_as<std::invoke_result_t<Func, Args...>, T>)
    static auto invoke(Func&& func, Args&&... args) noexcept {
        try {
            return result(func(std::forward<Args>(args)...));
        } catch (...) {
            return result(std::current_exception());
        }
    }
};

template <typename Func, typename... Args, typename T = std::invoke_result_t<Func, Args...>>
inline result<T> invoke_result(Func&& func, Args&&... args) {
    return result<T>::invoke(std::forward<Func>(func), std::forward<Args>(args)...);
}

} // namespace seastar
