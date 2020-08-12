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

#include <iterator>
#include <memory>
#include <vector>

#include <seastar/core/future.hh>

namespace seastar {

/// \addtogroup future-util
/// @{

namespace internal {
template <typename Iterator, typename AsyncAction>
class do_for_each_state final : public continuation_base<> {
    Iterator _begin;
    Iterator _end;
    AsyncAction _action;
    promise<> _pr;

public:
    do_for_each_state(Iterator begin, Iterator end, AsyncAction action, future<> first_unavailable)
        : _begin(std::move(begin)), _end(std::move(end)), _action(std::move(action)) {
        internal::set_callback(first_unavailable, this);
    }
    virtual void run_and_dispose() noexcept override {
        std::unique_ptr<do_for_each_state> zis(this);
        if (_state.failed()) {
            _pr.set_urgent_state(std::move(_state));
            return;
        }
        while (_begin != _end) {
            auto f = futurize_invoke(_action, *_begin++);
            if (f.failed()) {
                f.forward_to(std::move(_pr));
                return;
            }
            if (!f.available() || need_preempt()) {
                _state = {};
                internal::set_callback(f, this);
                zis.release();
                return;
            }
        }
        _pr.set_value();
    }
    task* waiting_task() noexcept override {
        return _pr.waiting_task();
    }
    future<> get_future() {
        return _pr.get_future();
    }
};

template<typename Iterator, typename AsyncAction>
inline
future<> do_for_each_impl(Iterator begin, Iterator end, AsyncAction action) {
    while (begin != end) {
        auto f = futurize_invoke(action, *begin++);
        if (f.failed()) {
            return f;
        }
        if (!f.available() || need_preempt()) {
            auto* s = new internal::do_for_each_state<Iterator, AsyncAction>{
                std::move(begin), std::move(end), std::move(action), std::move(f)};
            return s->get_future();
        }
    }
    return make_ready_future<>();
}
} // namespace internal

/// \addtogroup future-util

/// \brief Call a function for each item in a range, sequentially (iterator version).
///
/// For each item in a range, call a function, waiting for the previous
/// invocation to complete before calling the next one.
///
/// \param begin an \c InputIterator designating the beginning of the range
/// \param end an \c InputIterator designating the endof the range
/// \param action a callable, taking a reference to objects from the range
///               as a parameter, and returning a \c future<> that resolves
///               when it is acceptable to process the next item.
/// \return a ready future on success, or the first failed future if
///         \c action failed.
template<typename Iterator, typename AsyncAction>
SEASTAR_CONCEPT( requires requires (Iterator i, AsyncAction aa) {
    { futurize_invoke(aa, *i) } -> std::same_as<future<>>;
} )
inline
future<> do_for_each(Iterator begin, Iterator end, AsyncAction action) noexcept {
    try {
        return internal::do_for_each_impl(std::move(begin), std::move(end), std::move(action));
    } catch (...) {
        return current_exception_as_future();
    }
}

/// \brief Call a function for each item in a range, sequentially (range version).
///
/// For each item in a range, call a function, waiting for the previous
/// invocation to complete before calling the next one.
///
/// \param c an \c Container object designating input range
/// \param action a callable, taking a reference to objects from the range
///               as a parameter, and returning a \c future<> that resolves
///               when it is acceptable to process the next item.
/// \return a ready future on success, or the first failed future if
///         \c action failed.
template<typename Container, typename AsyncAction>
SEASTAR_CONCEPT( requires requires (Container c, AsyncAction aa) {
    { futurize_invoke(aa, *c.begin()) } -> std::same_as<future<>>;
} )
inline
future<> do_for_each(Container& c, AsyncAction action) noexcept {
    try {
        return internal::do_for_each_impl(std::begin(c), std::end(c), std::move(action));
    } catch (...) {
        return current_exception_as_future();
    }
}

namespace internal {

template <typename Iterator, typename IteratorCategory>
inline
size_t
iterator_range_estimate_vector_capacity(Iterator begin, Iterator end, IteratorCategory category) {
    // For InputIterators we can't estimate needed capacity
    return 0;
}

template <typename Iterator>
inline
size_t
iterator_range_estimate_vector_capacity(Iterator begin, Iterator end, std::forward_iterator_tag category) {
    // May be linear time below random_access_iterator_tag, but still better than reallocation
    return std::distance(begin, end);
}

} // namespace internal

/// \cond internal

class parallel_for_each_state final : private continuation_base<> {
    std::vector<future<>> _incomplete;
    promise<> _result;
    std::exception_ptr _ex;
private:
    // Wait for one of the futures in _incomplete to complete, and then
    // decide what to do: wait for another one, or deliver _result if all
    // are complete.
    void wait_for_one() noexcept;
    virtual void run_and_dispose() noexcept override;
    task* waiting_task() noexcept override { return _result.waiting_task(); }
public:
    parallel_for_each_state(size_t n);
    void add_future(future<>&& f);
    future<> get_future();
};

/// \endcond

/// \brief Run tasks in parallel (iterator version).
///
/// Given a range [\c begin, \c end) of objects, run \c func on each \c *i in
/// the range, and return a future<> that resolves when all the functions
/// complete.  \c func should return a future<> that indicates when it is
/// complete.  All invocations are performed in parallel. This allows the range
/// to refer to stack objects, but means that unlike other loops this cannot
/// check need_preempt and can only be used with small ranges.
///
/// \param begin an \c InputIterator designating the beginning of the range
/// \param end an \c InputIterator designating the end of the range
/// \param func Function to invoke with each element in the range (returning
///             a \c future<>)
/// \return a \c future<> that resolves when all the function invocations
///         complete.  If one or more return an exception, the return value
///         contains one of the exceptions.
template <typename Iterator, typename Func>
SEASTAR_CONCEPT( requires requires (Func f, Iterator i) { { f(*i++) } -> std::same_as<future<>>; } )
inline
future<>
parallel_for_each(Iterator begin, Iterator end, Func&& func) noexcept {
    parallel_for_each_state* s = nullptr;
    // Process all elements, giving each future the following treatment:
    //   - available, not failed: do nothing
    //   - available, failed: collect exception in ex
    //   - not available: collect in s (allocating it if needed)
    while (begin != end) {
        auto f = futurize_invoke(std::forward<Func>(func), *begin++);
        if (!f.available() || f.failed()) {
            if (!s) {
                memory::disable_failure_guard dfg;
                using itraits = std::iterator_traits<Iterator>;
                auto n = (internal::iterator_range_estimate_vector_capacity(begin, end, typename itraits::iterator_category()) + 1);
                s = new parallel_for_each_state(n);
            }
            s->add_future(std::move(f));
        }
    }
    // If any futures were not available, hand off to parallel_for_each_state::start().
    // Otherwise we can return a result immediately.
    if (s) {
        // s->get_future() takes ownership of s (and chains it to one of the futures it contains)
        // so this isn't a leak
        return s->get_future();
    }
    return make_ready_future<>();
}

/// \brief Run tasks in parallel (range version).
///
/// Given a \c range of objects, invoke \c func with each object
/// in the range, and return a future<> that resolves when all
/// the functions complete.  \c func should return a future<> that indicates
/// when it is complete.  All invocations are performed in parallel. This allows
/// the range to refer to stack objects, but means that unlike other loops this
/// cannot check need_preempt and can only be used with small ranges.
///
/// \param range A range of objects to iterate run \c func on
/// \param func  A callable, accepting reference to the range's
///              \c value_type, and returning a \c future<>.
/// \return a \c future<> that becomes ready when the entire range
///         was processed.  If one or more of the invocations of
///         \c func returned an exceptional future, then the return
///         value will contain one of those exceptions.

namespace internal {

template <typename Range, typename Func>
inline
future<>
parallel_for_each_impl(Range&& range, Func&& func) {
    return parallel_for_each(std::begin(range), std::end(range),
            std::forward<Func>(func));
}

} // namespace internal

template <typename Range, typename Func>
SEASTAR_CONCEPT( requires requires (Func f, Range r) { { f(*r.begin()) } -> std::same_as<future<>>; } )
inline
future<>
parallel_for_each(Range&& range, Func&& func) noexcept {
    auto impl = internal::parallel_for_each_impl<Range, Func>;
    return futurize_invoke(impl, std::forward<Range>(range), std::forward<Func>(func));
}

/// @}

} // namespace seastar
