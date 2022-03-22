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
 * Copyright (C) 2019 ScyllaDB Ltd.
 */

#include <iostream>
#include <fmt/printf.h>

#include <seastar/util/std-compat.hh>

#ifndef SEASTAR_COROUTINES_ENABLED

int main(int argc, char** argv) {
    std::cout << "coroutines not available\n";
    return 0;
}

#else

#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/fstream.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/sstring.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/immediate.hh>

seastar::future<> write_str_to_file(seastar::sstring file_name, seastar::sstring str) noexcept {
    auto file_imm = co_await seastar::coroutine::immediate(seastar::open_file_dma(file_name, seastar::open_flags::create | seastar::open_flags::wo));
    if (file_imm.failed()) {
        fmt::printf("Could not open {}: {}", file_name, file_imm.get_exception());
        co_return;
    }
    auto out = co_await seastar::make_file_output_stream(std::move(file_imm.get()));
    auto write_res = co_await seastar::coroutine::immediate([&] () -> seastar::future<> {
        co_await out.write(str);
        co_await out.flush();
    });
    co_await out.close();
    if (write_res.failed()) {
        fmt::printf("Could not write to {}: {}", file_name, write_res.get_exception());
    }
}

int main(int argc, char** argv) {
    seastar::app_template app;
    app.run(argc, argv, [] () -> seastar::future<> {
        std::cout << "this is a completely useless program\nplease stand by...\n";
        auto f = seastar::coroutine::parallel_for_each(std::vector<int> { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }, [] (int i) -> seastar::future<> {
            co_await seastar::sleep(std::chrono::seconds(i));
            std::cout << i << "\n";
        });

        co_await write_str_to_file("useless_file.txt", "nothing to see here, move along now\n");

        bool all_exist = true;
        std::vector<seastar::sstring> filenames = { "useless_file.txt", "non_existing" };
        co_await seastar::coroutine::parallel_for_each(filenames, [&all_exist] (const seastar::sstring& name) -> seastar::future<> {
            all_exist &= co_await seastar::file_exists(name);
        });
        std::cout << (all_exist ? "" : "not ") << "all files exist" << std::endl;

        co_await std::move(f);
        std::cout << "done\n";
    });
}

#endif
