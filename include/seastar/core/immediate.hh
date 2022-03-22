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

#include <seastar/core/future.hh>

namespace seastar {

namespace internal {

class immediate_base {
public:
    enum class state {
        unavailable,
        value,
        failed
    };

private:
    state _state = state::unavailable;
    std::exception_ptr _ex = nullptr;

public:
    explicit immediate_base() = default;

    struct with_value_tag {};
    explicit immediate_base(with_value_tag) noexcept
        : _state(state::value)
    {}
    explicit immediate_base(std::exception_ptr ex) noexcept
        : _state(state::failed)
        , _ex(std::move(ex))
    {};
    immediate_base(immediate_base&& o) noexcept
        : _state(std::exchange(o._state, state::unavailable))
        , _ex(std::move(o._ex))
    {}

    ~immediate_base();

    immediate_base& operator=(immediate_base&& o) {
        if (this != &o) {
            _state = std::exchange(o._state, state::unavailable);
            _ex = std::move(o._ex);
        }
        return *this;
    }

    const bool has_value() const noexcept { return _state == state::value; }
    const bool failed() const noexcept { return bool(_ex); }
    const bool available() const noexcept { return has_value() || failed() ; }

    void get() {
        auto old_state = std::exchange(_state, state::unavailable);
        switch (old_state) {
        case state::value:
            return;
        case state::failed:
            std::rethrow_exception(std::move(_ex));
            break;
        default:
            throw std::runtime_error("immediate not available");
        }
    }

    void set_value() {
        if (_state != state::unavailable) {
            throw std::runtime_error("cannot set immediate value");
        }
        _state = state::value;
    }

    std::exception_ptr get_exception() noexcept {
        assert(_state == state::failed);
        _state = state::unavailable;
        return std::move(_ex);
    }

    void set_exception(std::exception_ptr ex) {
        if (_state != state::unavailable) {
            throw std::runtime_error("cannot set immediate exception");
        }
        _state = state::failed;
        _ex = std::move(ex);
    }
};

} // namespace internal

template <class T = void>
class immediate;

/// \addtogroup fiber-module
/// @{

template<>
class SEASTAR_NODISCARD immediate<void> final : public internal::immediate_base {
public:
    using value_type = void;

    explicit immediate() = default;
    explicit immediate(internal::immediate_base::with_value_tag t) noexcept
        : internal::immediate_base(t)
    {}
    explicit immediate(std::exception_ptr ex) noexcept
        : internal::immediate_base(std::move(ex))
    {}
    template <typename Exception>
    explicit immediate(Exception&& e) noexcept
        : internal::immediate_base(std::make_exception_ptr(std::move(e)))
    {}
    explicit immediate(future<>&& fut)
        : internal::immediate_base()
    {
        if (!fut.available()) {
            throw std::runtime_error("future not available");
        }
        if (fut.failed()) {
            set_exception(std::move(fut).get_exception());
        } else {
            internal::immediate_base::set_value();
        }
    }
    immediate(immediate&&) = default;

    immediate& operator=(immediate&& o) {
        if (this != &o) {
            this->internal::immediate_base::operator=(std::move(*dynamic_cast<internal::immediate_base*>(&o)));
        }
        return *this;
    }

    void set_value() {
        internal::immediate_base::set_value();
    }
};

/// \brief A representation of a value or an error.
///
/// An \c immediate contains an optional value or an error.
/// It can be in one of several
/// states:
///    - unavailable: the immediate has not been set a value or an error yet.
///    - value: the immediate value is available.
///    - failed: the immediate was set with an error
///
/// An \ref immediate should not be discarded before
/// its value or exception are extracted. Discarding an \ref immediate means that the
/// computed value becomes inaccessible, but more importantly, any
/// exceptions raised from the computation will disappear unchecked as
/// well.
/// To prevent accidental discarding of immediates, \ref immediate is
/// declared `[[nodiscard]]` if the compiler supports it. Also, when a
/// discarded \ref immediate resolves with an error a warning is logged
/// (at runtime).
///
/// \tparam T A type to be carried as the value of the immediate,
///           similar to \c std::tuple<T...>. An empty or void type (\c immediate<>)
///           means that there is no value, and an available immediate only
///           contains a success/failure indication (and in the case of a
///           failure, an exception).
template <class T>
class SEASTAR_NODISCARD immediate final : public internal::immediate_base {
    std::optional<T> _value = std::nullopt;
public:
    using value_type = T;

    immediate() = default;
    explicit immediate(T value)
        : internal::immediate_base()
        , _value(std::move(value))
    {
        internal::immediate_base::set_value();
    }
    explicit immediate(std::exception_ptr ex) noexcept
        : internal::immediate_base(std::move(ex))
    {}
    template <typename Exception>
    explicit immediate(Exception&& e) noexcept
        : internal::immediate_base(std::make_exception_ptr(std::move(e)))
    {}
    explicit immediate(future<T>&& fut)
        : internal::immediate_base()
    {
        if (!fut.available()) {
            throw std::runtime_error("future not available");
        }
        if (fut.failed()) {
            set_exception(std::move(fut).get_exception());
        } else {
            _value.emplace(std::move(std::move(fut).get0()));
            internal::immediate_base::set_value();
        }
    }
    immediate(immediate&&) = default;

    immediate& operator=(immediate&& o) {
        if (this != &o) {
            this->internal::immediate_base::operator=(std::move(*dynamic_cast<internal::immediate_base*>(&o)));
            _value = std::move(o._value);
        }
        return *this;
    }

    const T& get() const {
        internal::immediate_base::get();
        return *_value;
    }
    T& get() {
        internal::immediate_base::get();
        return *_value;
    }

    void set_value(T&& value) {
        internal::immediate_base::set_value();
        _value = std::move(value);
    }
};

/// Make an \ref immediate containing a value constructed with \c args.
/// and return the immediate result.
template <typename T = void, typename... Args>
immediate<T> make_immediate(Args... args) {
    if constexpr (std::is_void_v<T>) {
        return immediate<>(internal::immediate_base::with_value_tag{});
    } else {
        return immediate<T>(std::forward<Args>(args)...);
    }
}

/// \brief Check whether a type is an immediate
///
/// This is a type trait evaluating to \c true if the given type is
/// an immediate.
template <typename... T> struct is_immediate : std::false_type {};

template <typename... T> struct is_immediate<immediate<T...>> : std::true_type {};

template <typename... T>
inline constexpr bool is_immediate_v = is_immediate<T...>::value;

/// \cond internal

namespace internal {

template <typename T>
struct immediatize_base {
    /// If \c T is a future, \c T; otherwise \c future<T>
    using type = immediate<T>;

    /// Convert a value or an immediate to an immediate
    static inline type convert(T&& value) { return immediate<T>(std::move(value)); }
    static inline type convert(type&& value) { return std::move(value); }

    static immediate<T> current_exception_as_immediate() noexcept {
        return immediate<T>(std::current_exception());
    }
};

template <>
struct immediatize_base<void> {
    using type = immediate<>;

    static inline type convert(type&& value) {
        return std::move(value);
    }

    static immediate<> current_exception_as_immediate() noexcept {
        return immediate<>(std::current_exception());
    }
};

template <typename T>
struct immediatize_base<immediate<T>> : public immediatize_base<T> {};

template <>
struct immediatize_base<immediate<>> : public immediatize_base<void> {};

} // namespace internal

template <typename T = void>
struct immediatize : public internal::immediatize_base<T> {
    using base = internal::immediatize_base<T>;
    using type = typename base::type;
    /// The value tuple type associated with \c type
    using value_type = typename type::value_type;
    using base::convert;

    /// Invoke a function to an argument list
    /// and return the result, as an immediate (if it wasn't already).
    template<typename Func, typename... FuncArgs>
    static inline type invoke(Func&& func, FuncArgs&&... args) noexcept;

    template<typename Func>
    static inline type invoke(Func&& func, internal::monostate) noexcept {
        return invoke(std::forward<Func>(func));
    }
};

template<typename T>
template<typename Func, typename... FuncArgs>
typename immediatize<T>::type immediatize<T>::invoke(Func&& func, FuncArgs&&... args) noexcept {
    using ret_t = decltype(func(std::forward<FuncArgs>(args)...));
    if constexpr (is_immediate_v<ret_t> && noexcept(func(args...))) {
        return func(std::forward<FuncArgs>(args)...);
    }
    try {
        if constexpr (std::is_void_v<ret_t>) {
            func(std::forward<FuncArgs>(args)...);
            return immediate<>(internal::immediate_base::with_value_tag{});
        } else if constexpr (is_immediate_v<ret_t>) {
            return func(std::forward<FuncArgs>(args)...);
        } else {
            return convert(func(std::forward<FuncArgs>(args)...));
        }
    } catch (...) {
        return immediatize<T>::current_exception_as_immediate();
    }
}

/// \endcond

/// Invoke a function to an argument list
/// and return the result, as an immediate (if it wasn't already).
template <typename Func, typename... Args, typename T = typename immediatize<std::invoke_result_t<Func, Args...>>::value_type>
inline immediate<T> immediate_invoke(Func&& func, Args&&... args) {
    return immediatize<T>::invoke(std::forward<Func>(func), std::forward<Args>(args)...);
}

/// \brief Wrap a \ref future<T> in an \ref immediate<T>
///
/// Useful for avoiding a exception when co_await:ing
/// the result.
///
/// \param fut A future
/// \return a future holding an \ref immediate value
template <typename T = void>
SEASTAR_CONCEPT(requires (!is_immediate_v<T>))
future<immediate<T>> make_future_immediate(future<T>&& fut) {
    if (fut.available()) {
        return make_ready_future<immediate<T>>(immediate<T>(std::move(fut)));
    } else {
        return std::move(fut).then_wrapped([] (future<T> fut) {
            return make_ready_future<immediate<T>>(immediate<T>(std::move(fut)));
        });
    }
}

/// @}

} // namespace seastar
