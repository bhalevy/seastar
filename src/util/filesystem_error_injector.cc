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

#include <ostream>

#include <seastar/core/reactor.hh>
#include <seastar/util/log.hh>
#include <seastar/util/exceptions.hh>

#include <seastar/util/filesystem_error_injector.hh>

namespace seastar {

logger fsei_logger("filesystem_error_injector");

namespace filesystem_error_injector {

#ifdef SEASTAR_ENABLE_FILESYSTEM_ERROR_INJECTION

manager& local_filesystem_error_injector_manager() {
    return engine().get_filesystem_error_injector_manager();
}

std::ostream& operator<<(std::ostream& out, syscall_type type) {
    switch (type) {
    case syscall_type::open:      out << "open"; break;
    case syscall_type::remove:    out << "remove"; break;
    case syscall_type::rename:    out << "rename"; break;
    case syscall_type::link:      out << "link"; break;
    case syscall_type::chmod:     out << "chmod"; break;
    case syscall_type::stat:      out << "stat"; break;
    case syscall_type::access:    out << "access"; break;
    case syscall_type::statfs:    out << "statfs"; break;
    case syscall_type::statvfs:   out << "statvfs"; break;
    case syscall_type::mkdir:     out << "mkdir"; break;
    case syscall_type::fdatasync: out << "fdatasync"; break;
    case syscall_type::read:      out << "read"; break;
    case syscall_type::write:     out << "write"; break;
    default:
        out << "syscall_type(" << unsigned(type) << ')';
        break;
    }

    return out;
}

injector::injector(syscall_type type, int error, predicate_func func)
    : _type(type)
    , _default_error(error)
    , _func(std::move(func))
{
    register_injector(type, this);
}

injector::~injector() {
    unregister_injector(_type, this);
}

void manager::register_injector(syscall_type type, injector* i) {
    struct syscall_category {
        syscall_type type;
        bool read_like;
        bool write_like;
    };
    static std::initializer_list<syscall_category> all_syscall_types = {
        { syscall_type::open, true, true },
        { syscall_type::remove, false, true },
        { syscall_type::rename, false, true },
        { syscall_type::link, false, true },
        { syscall_type::chmod, false, true },
        { syscall_type::stat, true, false },
        { syscall_type::access, true, false },
        { syscall_type::statfs, true, false },
        { syscall_type::statvfs, true, false },
        { syscall_type::mkdir, false, true },
        { syscall_type::fdatasync, false, true },
        { syscall_type::read, true, false },
        { syscall_type::write, false, true },
    };
    if (type == syscall_type::all) {
        for (auto& t : all_syscall_types) {
            register_injector(t.type, i);
        }
        return;
    } else if (type == syscall_type::read_like) {
        for (auto& t : all_syscall_types) {
            if (t.read_like) {
                register_injector(t.type, i);
            }
        }
        return;
    } else if (type == syscall_type::write_like) {
        for (auto& t : all_syscall_types) {
            if (t.write_like) {
                register_injector(t.type, i);
            }
        }
        return;
    }

    if (i) {
        fsei_logger.debug("manager[{}] registering injector {} for {}", id(), i, type);
        if (registered_for_type(type)) {
            throw std::runtime_error(format("injector for type {} is already registered", type));
        }
    } else {
        fsei_logger.debug("manager[{}] unregistering injector for {} ({})", id(), type, int(type));
    }
    _registered[type] = i;
}

std::optional<ssize_t> injector::inject(std::optional<sstring> path1, std::optional<sstring> path2, ulong flags) noexcept {
    if (!_func(path1, path2, flags)) {
        return std::nullopt;
    }
    auto now = _count++;
    if (now >= _fail_at && now < _fail_until) {
        return std::optional<ssize_t>(-1);
    } else {
        return std::nullopt;
    }
}

std::optional<ssize_t> manager::on_syscall(syscall_type type, std::optional<sstring> path1, std::optional<sstring> path2, ulong flags) noexcept {
    if (_suppressed) {
        fsei_logger.trace("manager[{}] not injected error for {}: suppressed={}", id(), type, _suppressed);
        return std::nullopt;
    }
    injector* ip = _registered[type];
    if (!ip) {
        fsei_logger.trace("manager[{}] no injector registered for {}", id(), type);
        return std::nullopt;
    }
    return on_syscall(*ip, type, std::move(path1), std::move(path2), flags);
}

std::optional<ssize_t> manager::on_syscall(injector& i, syscall_type type, std::optional<sstring> path1, std::optional<sstring> path2, ulong flags) noexcept {
    auto ret = i.inject(path1, path2, flags);
    if (ret) {
        fsei_logger.debug("manager[{}] injector[{}] injecting error for {}: error={} path1={} path2={} flags={}: ret={}",
                id(), i.description(), type, i.error(), path1.value_or(""), path2.value_or(""), flags, *ret);
        errno = i.error();
        return ret;
    } else {
        fsei_logger.trace("manager[{}] injector[{}] not injected error for {}: error={} count={} fail_at={} fail_until={}",
                id(), i.description(), type, i.error(), i.count(), i.fail_at(), i.fail_until());
    }
    return std::nullopt;
}

#endif // SEASTAR_ENABLE_FILESYSTEM_ERROR_INJECTION

} // namespace filesystem_error_injector
} // namespace seastar
