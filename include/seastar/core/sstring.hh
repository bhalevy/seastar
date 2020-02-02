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
 * Copyright 2014 Cloudius Systems
 */

#pragma once

#include <stdint.h>
#include <algorithm>
#include <string>
#include <vector>
#include <unordered_map>
#include <cstring>
#include <stdexcept>
#include <initializer_list>
#include <istream>
#include <ostream>
#include <functional>
#include <cstdio>
#include <type_traits>
#include <seastar/util/std-compat.hh>
#include <seastar/core/temporary_buffer.hh>

namespace seastar {

template <typename char_type, typename Size, Size max_size, bool NulTerminate = true>
class basic_sstring;

using sstring = basic_sstring<char, uint32_t, 15>;

template <typename string_type = sstring, typename T>
inline string_type to_sstring(T value);

template <typename char_type, typename Size, Size max_size, bool NulTerminate>
class basic_sstring {
    static_assert(
            (std::is_same<char_type, char>::value
             || std::is_same<char_type, signed char>::value
             || std::is_same<char_type, unsigned char>::value),
            "basic_sstring only supports single byte char types");
    union contents {
        struct external_type {
            char_type* str;
            Size size;
            int8_t pad;
        } external;
        struct internal_type {
            char_type str[max_size];
            int8_t size;
        } internal;
        static_assert(sizeof(external_type) <= sizeof(internal_type), "max_size too small");
        static_assert(max_size <= 127, "max_size too large");
    } u;
    bool is_internal() const noexcept {
        return u.internal.size >= 0;
    }
    bool is_external() const noexcept {
        return !is_internal();
    }
    const char_type* str() const {
        return is_internal() ? u.internal.str : u.external.str;
    }
    char_type* str() {
        return is_internal() ? u.internal.str : u.external.str;
    }

    template <typename string_type, typename T>
    static inline string_type to_sstring_sprintf(T value, const char* fmt) {
        char tmp[sizeof(value) * 3 + 2];
        auto len = std::sprintf(tmp, fmt, value);
        using ch_type = typename string_type::value_type;
        return string_type(reinterpret_cast<ch_type*>(tmp), len);
    }

    template <typename string_type>
    static inline string_type to_sstring(int value) {
        return to_sstring_sprintf<string_type>(value, "%d");
    }

    template <typename string_type>
    static inline string_type to_sstring(unsigned value) {
        return to_sstring_sprintf<string_type>(value, "%u");
    }

    template <typename string_type>
    static inline string_type to_sstring(long value) {
        return to_sstring_sprintf<string_type>(value, "%ld");
    }

    template <typename string_type>
    static inline string_type to_sstring(unsigned long value) {
        return to_sstring_sprintf<string_type>(value, "%lu");
    }

    template <typename string_type>
    static inline string_type to_sstring(long long value) {
        return to_sstring_sprintf<string_type>(value, "%lld");
    }

    template <typename string_type>
    static inline string_type to_sstring(unsigned long long value) {
        return to_sstring_sprintf<string_type>(value, "%llu");
    }

    template <typename string_type>
    static inline string_type to_sstring(float value) {
        return to_sstring_sprintf<string_type>(value, "%g");
    }

    template <typename string_type>
    static inline string_type to_sstring(double value) {
        return to_sstring_sprintf<string_type>(value, "%g");
    }

    template <typename string_type>
    static inline string_type to_sstring(long double value) {
        return to_sstring_sprintf<string_type>(value, "%Lg");
    }

    template <typename string_type>
    static inline string_type to_sstring(const char* value) {
        return string_type(value);
    }

    template <typename string_type>
    static inline string_type to_sstring(sstring value) {
        return value;
    }

    template <typename string_type>
    static inline string_type to_sstring(const temporary_buffer<char>& buf) {
        return string_type(buf.get(), buf.size());
    }
public:
    using value_type = char_type;
    using traits_type = std::char_traits<char_type>;
    using allocator_type = std::allocator<char_type>;
    using reference = char_type&;
    using const_reference = const char_type&;
    using pointer = char_type*;
    using const_pointer = const char_type*;
    using iterator = char_type*;
    using const_iterator = const char_type*;
    // FIXME: add reverse_iterator and friend
    using difference_type = ssize_t;  // std::make_signed_t<Size> can be too small
    using size_type = Size;
    static constexpr size_type  npos = static_cast<size_type>(-1);
    static constexpr unsigned padding() { return unsigned(NulTerminate); }
public:
    struct initialized_later {};

    basic_sstring() noexcept {
        u.internal.size = 0;
        if (NulTerminate) {
            u.internal.str[0] = '\0';
        }
    }
    basic_sstring(const basic_sstring& x) {
        if (x.is_internal()) {
            u.internal = x.u.internal;
        } else {
            u.internal.size = -1;
            u.external.str = reinterpret_cast<char_type*>(std::malloc(x.u.external.size + padding()));
            if (!u.external.str) {
                throw std::bad_alloc();
            }
            std::copy(x.u.external.str, x.u.external.str + x.u.external.size + padding(), u.external.str);
            u.external.size = x.u.external.size;
        }
    }
    basic_sstring(basic_sstring&& x) noexcept {
#pragma GCC diagnostic push
        // Is a small-string construction is followed by this move constructor, then the trailing bytes
        // of x.u are not initialized, but copied. gcc complains, but it is both legitimate to copy
        // these bytes, and more efficient than a variable-size copy
#pragma GCC diagnostic ignored "-Wuninitialized"
        u = x.u;
#pragma GCC diagnostic pop
        x.u.internal.size = 0;
        x.u.internal.str[0] = '\0';
    }
    basic_sstring(initialized_later, size_t size) {
        if (size_type(size) != size) {
            throw std::overflow_error("sstring overflow");
        }
        if (size + padding() <= sizeof(u.internal.str)) {
            if (NulTerminate) {
                u.internal.str[size] = '\0';
            }
            u.internal.size = size;
        } else {
            u.internal.size = -1;
            u.external.str = reinterpret_cast<char_type*>(std::malloc(size + padding()));
            if (!u.external.str) {
                throw std::bad_alloc();
            }
            u.external.size = size;
            if (NulTerminate) {
                u.external.str[size] = '\0';
            }
        }
    }
    basic_sstring(const char_type* x, size_t size) {
        if (size_type(size) != size) {
            throw std::overflow_error("sstring overflow");
        }
        if (size + padding() <= sizeof(u.internal.str)) {
            std::copy(x, x + size, u.internal.str);
            if (NulTerminate) {
                u.internal.str[size] = '\0';
            }
            u.internal.size = size;
        } else {
            u.internal.size = -1;
            u.external.str = reinterpret_cast<char_type*>(std::malloc(size + padding()));
            if (!u.external.str) {
                throw std::bad_alloc();
            }
            u.external.size = size;
            std::copy(x, x + size, u.external.str);
            if (NulTerminate) {
                u.external.str[size] = '\0';
            }
        }
    }

    basic_sstring(size_t size, char_type x) : basic_sstring(initialized_later(), size) {
        memset(begin(), x, size);
    }

    basic_sstring(const char* x) : basic_sstring(reinterpret_cast<const char_type*>(x), std::strlen(x)) {}
    basic_sstring(std::basic_string<char_type>& x) : basic_sstring(x.c_str(), x.size()) {}
    basic_sstring(std::initializer_list<char_type> x) : basic_sstring(x.begin(), x.end() - x.begin()) {}
    basic_sstring(const char_type* b, const char_type* e) : basic_sstring(b, e - b) {}
    basic_sstring(const std::basic_string<char_type>& s)
        : basic_sstring(s.data(), s.size()) {}
    template <typename InputIterator>
    basic_sstring(InputIterator first, InputIterator last)
            : basic_sstring(initialized_later(), std::distance(first, last)) {
        std::copy(first, last, begin());
    }
    explicit basic_sstring(compat::basic_string_view<char_type, traits_type> v)
            : basic_sstring(v.data(), v.size()) {
    }
    ~basic_sstring() noexcept {
        if (is_external()) {
            std::free(u.external.str);
        }
    }
    basic_sstring& operator=(const basic_sstring& x) {
        basic_sstring tmp(x);
        swap(tmp);
        return *this;
    }
    basic_sstring& operator=(basic_sstring&& x) noexcept {
        if (this != &x) {
            swap(x);
            x.reset();
        }
        return *this;
    }
    operator std::basic_string<char_type>() const {
        return { str(), size() };
    }

    size_t size() const noexcept {
        return is_internal() ? u.internal.size : u.external.size;
    }

    size_t length() const noexcept {
        return size();
    }

    size_t find(char_type t, size_t pos = 0) const noexcept {
        const char_type* it = str() + pos;
        const char_type* end = str() + size();
        while (it < end) {
            if (*it == t) {
                return it - str();
            }
            it++;
        }
        return npos;
    }

    size_t find(const basic_sstring& s, size_t pos = 0) const noexcept {
        const char_type* it = str() + pos;
        const char_type* end = str() + size();
        const char_type* c_str = s.str();
        const char_type* c_str_end = s.str() + s.size();

        while (it < end) {
            auto i = it;
            auto j = c_str;
            while ( i < end && j < c_str_end && *i == *j) {
                i++;
                j++;
            }
            if (j == c_str_end) {
                return it - str();
            }
            it++;
        }
        return npos;
    }

    /**
     * find_last_of find the last occurrence of c in the string.
     * When pos is specified, the search only includes characters
     * at or before position pos.
     *
     */
    size_t find_last_of (char_type c, size_t pos = npos) const noexcept {
        const char_type* str_start = str();
        if (size()) {
            if (pos >= size()) {
                pos = size() - 1;
            }
            const char_type* p = str_start + pos + 1;
            do {
                p--;
                if (*p == c) {
                    return (p - str_start);
                }
            } while (p != str_start);
        }
        return npos;
    }

    /**
     *  Append a C substring.
     *  @param s  The C string to append.
     *  @param n  The number of characters to append.
     *  @return  Reference to this string.
     */
    basic_sstring& append (const char_type* s, size_t n) {
        basic_sstring ret(initialized_later(), size() + n);
        std::copy(begin(), end(), ret.begin());
        std::copy(s, s + n, ret.begin() + size());
        *this = std::move(ret);
        return *this;
    }

    /**
     *  Resize string.
     *  @param n  new size.
     *  @param c  if n greater than current size character to fill newly allocated space with.
     */
    void resize(size_t n, const char_type c  = '\0') {
        if (n > size()) {
            extend(n - size(), c);
        } else if (n < size()) {
            shrink(n);
        }
    }

    /**
     *  Replace characters with a value of a C style substring.
     *
     */
    basic_sstring& replace(size_type pos, size_type n1, const char_type* s,
             size_type n2) {
        if (pos > size()) {
            throw std::out_of_range("sstring::replace out of range");
        }

        if (n1 > size() - pos) {
            n1 = size() - pos;
        }

        if (n1 == n2) {
            if (n2) {
                std::copy(s, s + n2, begin() + pos);
            }
            return *this;
        }
        basic_sstring ret(initialized_later(), size() + n2 - n1);
        char_type* p= ret.begin();
        std::copy(begin(), begin() + pos, p);
        p += pos;
        if (n2) {
            std::copy(s, s + n2, p);
        }
        p += n2;
        std::copy(begin() + pos + n1, end(), p);
        *this = std::move(ret);
        return *this;
    }

    template <class InputIterator>
    basic_sstring& replace (const_iterator i1, const_iterator i2,
            InputIterator first, InputIterator last) {
        if (i1 < begin() || i1 > end() || i2 < begin()) {
            throw std::out_of_range("sstring::replace out of range");
        }
        if (i2 > end()) {
            i2 = end();
        }

        if (i2 - i1 == last - first) {
            //in place replacement
            std::copy(first, last, const_cast<char_type*>(i1));
            return *this;
        }
        basic_sstring ret(initialized_later(), size() + (last - first) - (i2 - i1));
        char_type* p = ret.begin();
        p = std::copy(cbegin(), i1, p);
        p = std::copy(first, last, p);
        std::copy(i2, cend(), p);
        *this = std::move(ret);
        return *this;
    }

    iterator erase(iterator first, iterator last) {
        size_t pos = first - begin();
        replace(pos, last - first, nullptr, 0);
        return begin() + pos;
    }

    /**
     * Inserts additional characters into the string right before
     * the character indicated by p.
     */
    template <class InputIterator>
    void insert(const_iterator p, InputIterator beg, InputIterator end) {
        replace(p, p, beg, end);
    }

    /**
     *  Returns a read/write reference to the data at the last
     *  element of the string.
     *  This function shall not be called on empty strings.
     */
    reference
    back() noexcept {
        return operator[](size() - 1);
    }

    /**
     *  Returns a  read-only (constant) reference to the data at the last
     *  element of the string.
     *  This function shall not be called on empty strings.
     */
    const_reference
    back() const noexcept {
        return operator[](size() - 1);
    }

    basic_sstring substr(size_t from, size_t len = npos)  const {
        if (from > size()) {
            throw std::out_of_range("sstring::substr out of range");
        }
        if (len > size() - from) {
            len = size() - from;
        }
        if (len == 0) {
            return "";
        }
        return { str() + from , len };
    }

    const char_type& at(size_t pos) const {
        if (pos >= size()) {
            throw std::out_of_range("sstring::at out of range");
        }
        return *(str() + pos);
    }

    char_type& at(size_t pos) {
        if (pos >= size()) {
            throw std::out_of_range("sstring::at out of range");
        }
        return *(str() + pos);
    }

    bool empty() const noexcept {
        return u.internal.size == 0;
    }
    void reset() noexcept {
        if (is_external()) {
            std::free(u.external.str);
        }
        u.internal.size = 0;
        if (NulTerminate) {
            u.internal.str[0] = '\0';
        }
    }
    temporary_buffer<char_type> release() && {
        if (is_external()) {
            auto ptr = u.external.str;
            auto size = u.external.size;
            u.external.str = nullptr;
            u.external.size = 0;
            return temporary_buffer<char_type>(ptr, size, make_free_deleter(ptr));
        } else {
            auto buf = temporary_buffer<char_type>(u.internal.size);
            std::copy(u.internal.str, u.internal.str + u.internal.size, buf.get_write());
            u.internal.size = 0;
            if (NulTerminate) {
                u.internal.str[0] = '\0';
            }
            return buf;
        }
    }
    int compare(const basic_sstring& x) const noexcept {
        auto n = traits_type::compare(begin(), x.begin(), std::min(size(), x.size()));
        if (n != 0) {
            return n;
        }
        if (size() < x.size()) {
            return -1;
        } else if (size() > x.size()) {
            return 1;
        } else {
            return 0;
        }
    }

    int compare(size_t pos, size_t sz, const basic_sstring& x) const {
        if (pos > size()) {
            throw std::out_of_range("pos larger than string size");
        }

        sz = std::min(size() - pos, sz);
        auto n = traits_type::compare(begin() + pos, x.begin(), std::min(sz, x.size()));
        if (n != 0) {
            return n;
        }
        if (sz < x.size()) {
            return -1;
        } else if (sz > x.size()) {
            return 1;
        } else {
            return 0;
        }
    }

    void swap(basic_sstring& x) noexcept {
        contents tmp;
        tmp = x.u;
        x.u = u;
        u = tmp;
    }
    char_type* data() {
        return str();
    }
    const char_type* data() const {
        return str();
    }
    const char_type* c_str() const {
        return str();
    }
    const char_type* begin() const { return str(); }
    const char_type* end() const { return str() + size(); }
    const char_type* cbegin() const { return str(); }
    const char_type* cend() const { return str() + size(); }
    char_type* begin() { return str(); }
    char_type* end() { return str() + size(); }
    bool operator==(const basic_sstring& x) const {
        return size() == x.size() && std::equal(begin(), end(), x.begin());
    }
    bool operator!=(const basic_sstring& x) const {
        return !operator==(x);
    }
    bool operator<(const basic_sstring& x) const {
        return compare(x) < 0;
    }
    basic_sstring operator+(const basic_sstring& x) const {
        basic_sstring ret(initialized_later(), size() + x.size());
        std::copy(begin(), end(), ret.begin());
        std::copy(x.begin(), x.end(), ret.begin() + size());
        return ret;
    }
    basic_sstring& operator+=(const basic_sstring& x) {
        return *this = *this + x;
    }
    char_type& operator[](size_type pos) {
        return str()[pos];
    }
    const char_type& operator[](size_type pos) const {
        return str()[pos];
    }

    operator compat::basic_string_view<char_type>() const {
        return compat::basic_string_view<char_type>(str(), size());
    }

    template <typename string_type, typename T>
    friend inline string_type to_sstring(T value);
private:
    /*
     *  Extend string.
     *  @param count  number of characters to add.
     *  @param c  character to fill newly allocated space with.
     */
    void extend(size_t count, const char_type c) {
        *this += basic_sstring(count, c);
    }

    /**
     *  Shrink string.
     *  @param n  new size.
     */
    void shrink(size_t n) {
        if (is_internal()) {
            u.internal.size = n;
            if (NulTerminate) {
                u.internal.str[n] = '\0';
            }
        } else if (n + padding() <= sizeof(u.internal.str)) {
            *this = basic_sstring(u.external.str, n);
        } else {
            u.external.size = n;
            if (NulTerminate) {
                u.external.str[n] = '\0';
            }
        }
    }
};
template <typename char_type, typename Size, Size max_size, bool NulTerminate>
constexpr Size basic_sstring<char_type, Size, max_size, NulTerminate>::npos;

template <typename char_type, typename size_type, size_type Max, size_type N, bool NulTerminate>
inline
basic_sstring<char_type, size_type, Max, NulTerminate>
operator+(const char(&s)[N], const basic_sstring<char_type, size_type, Max, NulTerminate>& t) {
    using sstring = basic_sstring<char_type, size_type, Max, NulTerminate>;
    // don't copy the terminating NUL character
    sstring ret(typename sstring::initialized_later(), N-1 + t.size());
    auto p = std::copy(std::begin(s), std::end(s)-1, ret.begin());
    std::copy(t.begin(), t.end(), p);
    return ret;
}

template <size_t N>
static inline
size_t str_len(const char(&s)[N]) { return N - 1; }

template <size_t N>
static inline
const char* str_begin(const char(&s)[N]) { return s; }

template <size_t N>
static inline
const char* str_end(const char(&s)[N]) { return str_begin(s) + str_len(s); }

template <typename char_type, typename size_type, size_type max_size, bool NulTerminate>
static inline
const char_type* str_begin(const basic_sstring<char_type, size_type, max_size, NulTerminate>& s) { return s.begin(); }

template <typename char_type, typename size_type, size_type max_size, bool NulTerminate>
static inline
const char_type* str_end(const basic_sstring<char_type, size_type, max_size, NulTerminate>& s) { return s.end(); }

template <typename char_type, typename size_type, size_type max_size, bool NulTerminate>
static inline
size_type str_len(const basic_sstring<char_type, size_type, max_size, NulTerminate>& s) { return s.size(); }

template <typename First, typename Second, typename... Tail>
static inline
size_t str_len(const First& first, const Second& second, const Tail&... tail) {
    return str_len(first) + str_len(second, tail...);
}

template <typename char_type, typename size_type, size_type max_size>
inline
void swap(basic_sstring<char_type, size_type, max_size>& x,
          basic_sstring<char_type, size_type, max_size>& y) noexcept
{
    return x.swap(y);
}

template <typename char_type, typename size_type, size_type max_size, bool NulTerminate, typename char_traits>
inline
std::basic_ostream<char_type, char_traits>&
operator<<(std::basic_ostream<char_type, char_traits>& os,
        const basic_sstring<char_type, size_type, max_size, NulTerminate>& s) {
    return os.write(s.begin(), s.size());
}

template <typename char_type, typename size_type, size_type max_size, bool NulTerminate, typename char_traits>
inline
std::basic_istream<char_type, char_traits>&
operator>>(std::basic_istream<char_type, char_traits>& is,
        basic_sstring<char_type, size_type, max_size, NulTerminate>& s) {
    std::string tmp;
    is >> tmp;
    s = tmp;
    return is;
}

}

namespace std {

template <typename char_type, typename size_type, size_type max_size, bool NulTerminate>
struct hash<seastar::basic_sstring<char_type, size_type, max_size, NulTerminate>> {
    size_t operator()(const seastar::basic_sstring<char_type, size_type, max_size, NulTerminate>& s) const {
        return std::hash<seastar::compat::basic_string_view<char_type>>()(s);
    }
};

}

namespace seastar {

static inline
char* copy_str_to(char* dst) {
    return dst;
}

template <typename Head, typename... Tail>
static inline
char* copy_str_to(char* dst, const Head& head, const Tail&... tail) {
    return copy_str_to(std::copy(str_begin(head), str_end(head), dst), tail...);
}

template <typename String = sstring, typename... Args>
static String make_sstring(Args&&... args)
{
    String ret(sstring::initialized_later(), str_len(args...));
    copy_str_to(ret.begin(), args...);
    return ret;
}

template <typename string_type, typename T>
inline string_type to_sstring(T value) {
    return sstring::to_sstring<string_type>(value);
}

}

namespace std {
template <typename T>
inline
std::ostream& operator<<(std::ostream& os, const std::vector<T>& v) {
    bool first = true;
    os << "{";
    for (auto&& elem : v) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << elem;
    }
    os << "}";
    return os;
}

template <typename Key, typename T, typename Hash, typename KeyEqual, typename Allocator>
std::ostream& operator<<(std::ostream& os, const std::unordered_map<Key, T, Hash, KeyEqual, Allocator>& v) {
    bool first = true;
    os << "{";
    for (auto&& elem : v) {
        if (!first) {
            os << ", ";
        } else {
            first = false;
        }
        os << "{ " << elem.first << " -> " << elem.second << "}";
    }
    os << "}";
    return os;
}
}
