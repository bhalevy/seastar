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
 * Copyright 2020 ScyllaDB
 */

#pragma once

#include <cstddef>
#include <limits>
#include <optional>
#include <list>
#include <unordered_map>
#include <exception>
#include <ostream>

#include <seastar/core/future.hh>
#include <seastar/core/sstring.hh>
#include <seastar/util/log.hh>
#include <seastar/util/concepts.hh>

namespace seastar {

extern logger fsei_logger;

namespace filesystem_error_injector {

enum class syscall_type {
    open = 0,
    remove,
    rename,
    link,
    chmod,
    stat,
    access,
    statfs,
    statvfs,
    mkdir,
    fdatasync,
    read,
    write,

    all,
    read_like,
    write_like,
};

std::ostream& operator<<(std::ostream& out, syscall_type type);

#ifdef SEASTAR_ENABLE_FILESYSTEM_ERROR_INJECTION

///
/// Filesystem error injection framework. Allows testing syscall error handling.
///
/// To exhaustively inject failure at every syscall point:
///
///    uint64_t i = 0;
///    fsei::injector injector(EIO);
///    fsei::register_injector(fsei::syscall_type::all, &injector);
///    while (true) {
///        try {
///            injector.fail_after(i++);
///            code_under_test();
///            injector.cancel();
///            break;
///        } catch (...) {
///            // expected
///        }
///    }
///

class injector {
    using predicate_func = std::function<bool (std::optional<sstring>, std::optional<sstring>, ulong flags)>;

protected:
    syscall_type _type;
    int _error;
    uint64_t _count = 0;
    uint64_t _fail_at;
    uint64_t _fail_until;
    predicate_func _func;

public:
    injector(syscall_type type, int error, predicate_func func = [] (std::optional<sstring>, std::optional<sstring>, ulong) {
        return true;
    });
    injector(const injector&) = delete;
    injector(injector&&) = delete;

    // unregisters this injector
    virtual ~injector();

    // Will cause count-th allocation point from now to fail, counting from 0
    void fail_after(uint64_t count, uint64_t how_long = std::numeric_limits<uint64_t>::max()) noexcept {
        __int128 x = static_cast<__int128>(_count) + count;
        if (x >= static_cast<__int128>(std::numeric_limits<uint64_t>::max())) {
            cancel();
        } else {
            _fail_at = static_cast<uint64_t>(x);
            x += how_long;
            _fail_until = x <= static_cast<__int128>(std::numeric_limits<uint64_t>::max()) ?
                    static_cast<uint64_t>(x) : std::numeric_limits<uint64_t>::max();
        }
        fsei_logger.trace("injector[{}] armed: count={} fail_at={} fail_until={}", this, _count, _fail_at, _fail_until);
    }

    void fail() noexcept {
        fail_after(0);
    }

    void fail_for(uint64_t how_long) noexcept {
        fail_after(0, how_long);
    }

    void fail_once() noexcept {
        fail_for(1);
    }

    void fail_once_after(uint64_t count) noexcept {
        fail_after(count, 1);
    }

    void cancel() noexcept {
        _fail_until = _fail_at = std::numeric_limits<uint64_t>::max();
        fsei_logger.trace("injector[{}] cancelled: count={}", this, _count);
    }

    syscall_type type() const noexcept {
        return _type;
    }

    int error() const noexcept {
        return _error;
    }

    uint64_t count() const noexcept {
        return _count;
    }

    uint64_t fail_at() const noexcept {
        return _fail_at;
    }

    uint64_t fail_until() const noexcept {
        return _fail_until;
    }

    virtual sstring description() const {
        return "";
    }

    virtual std::optional<ssize_t> inject(std::optional<sstring> path1, std::optional<sstring> path2, ulong flags) noexcept;
};

class manager {
    unsigned _id;
    uint64_t _suppressed = 0;
    std::unordered_map<syscall_type, injector*> _registered = {};

    friend struct disable_guard;
public:
    manager(unsigned id = 0) noexcept
        : _id(id)
    { }

    void register_injector(syscall_type type, injector* i);

    void unregister_injector(syscall_type type, injector* i) {
        register_injector(type, nullptr);
    }

    unsigned id() const noexcept {
        return _id;
    }

    std::optional<ssize_t> on_syscall(syscall_type type, std::optional<sstring> path1, std::optional<sstring> path2, ulong flags) noexcept;

    void suppress() noexcept {
        ++_suppressed;
        fsei_logger.trace("manager[{}] suppressing injection: suppressed={}", id(), _suppressed);
    }

    void unsuppress() noexcept {
        --_suppressed;
        fsei_logger.trace("manager[{}] unsuppressing injection: suppressed={}", id(), _suppressed);
    }
private:
    injector* registered_for_type(syscall_type type) const {
        auto p = _registered.find(type);
        return p != _registered.end() ? p->second : nullptr;
    }

    std::optional<ssize_t> on_syscall(injector&, syscall_type type, std::optional<sstring> path1, std::optional<sstring> path2, ulong flags) noexcept;
};

manager& local_filesystem_error_injector_manager();

struct disable_guard {
    disable_guard() noexcept {
        local_filesystem_error_injector_manager().suppress();
    }
    disable_guard(const disable_guard&) noexcept {
        local_filesystem_error_injector_manager().suppress();
    }
    disable_guard(disable_guard&&) noexcept  {
        local_filesystem_error_injector_manager().suppress();
    }
    ~disable_guard() {
        local_filesystem_error_injector_manager().unsuppress();
    }
};

inline void register_injector(syscall_type type, injector* i) {
    auto& m = local_filesystem_error_injector_manager();
    m.register_injector(type, i);
}

inline void unregister_injector(syscall_type type, injector* i) {
    auto& m = local_filesystem_error_injector_manager();
    m.unregister_injector(type, i);
}

#else // SEASTAR_ENABLE_FILESYSTEM_ERROR_INJECTION

struct disable_guard {
};

template <typename... Args>
inline int on_syscall(syscall_type type, Args... args) noexcept {
    return 0;
}

#endif // SEASTAR_ENABLE_FILESYSTEM_ERROR_INJECTION

} // namespace filesystem_error_injector
} // namespace seastar
