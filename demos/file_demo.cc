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


// Demonstration of seastar::with_file

#include <cstring>
#include <iostream>
#include <limits>
#include <random>

#include <seastar/core/app-template.hh>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/file.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/sstring.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/util/tmp_file.hh>

using namespace seastar;

constexpr size_t aligned_size = 4096;

future<> verify_data_file(file& f, temporary_buffer<char>& rbuf, const temporary_buffer<char>& wbuf) {
    return f.dma_read(0, rbuf.get_write(), aligned_size).then([&rbuf, &wbuf] (size_t count) {
        assert(count == aligned_size);
        std::cout << "  verifying " << count << " bytes" << std::endl;
        assert(!memcmp(rbuf.get(), wbuf.get(), aligned_size));
    });
}

future<file> open_data_file(sstring meta_filename, temporary_buffer<char>& rbuf) {
    std::cout << "  retrieving data filename from " << meta_filename << std::endl;
    return with_file(open_file_dma(meta_filename, open_flags::ro), [&rbuf] (file& f) {
        return f.dma_read(0, rbuf.get_write(), aligned_size).then([&rbuf] (size_t count) {
            assert(count == aligned_size);
            auto data_filename = sstring(rbuf.get());
            std::cout << "  opening " << data_filename << std::endl;
            return open_file_dma(data_filename, open_flags::ro);
        });
    });
}

int main(int ac, char** av) {
    app_template app;
    return app.run(ac, av, [&] {
        return tmp_dir::do_with_thread([] (tmp_dir& t) {
            auto rnd = std::mt19937(std::random_device()());
            auto dist = std::uniform_int_distribution<char>(0, std::numeric_limits<char>::max());
            auto wbuf = temporary_buffer<char>::aligned(aligned_size, aligned_size);
            sstring meta_filename = (t.get_path() / "meta_file").native();
            sstring data_filename = (t.get_path() / "data_file").native();

            // print the data_filename into the write buffer
            std::fill(wbuf.get_write(), wbuf.get_write() + aligned_size, 0);
            std::copy(data_filename.cbegin(), data_filename.cend(), wbuf.get_write());

            // and write it to `meta_filename`
            std::cout << "writing \"" << data_filename << "\" into " << meta_filename << std::endl;

            // `with_file` is used to create/open `meta_filename` just around the call to `dma_write`
            auto count = with_file(open_file_dma(meta_filename, open_flags::rw | open_flags::create), [&wbuf] (file& f) {
                return f.dma_write(0, wbuf.get(), aligned_size);
            }).get0();
            assert(count == aligned_size);

            // now write some random data into data_filename
            std::cout << "writing random data into " << data_filename << std::endl;
            std::generate(wbuf.get_write(), wbuf.get_write() + aligned_size, [&dist, &rnd] { return dist(rnd); });

            // `with_file` is used to create/open `data_filename` just around the call to `dma_write`
            count = with_file(open_file_dma(data_filename, open_flags::rw | open_flags::create), [&wbuf] (file& f) {
                return f.dma_write(0, wbuf.get(), aligned_size);
            }).get0();
            assert(count == aligned_size);

            // verify the data via meta_filename
            std::cout << "verifying data..." << std::endl;
            auto rbuf = temporary_buffer<char>::aligned(aligned_size, aligned_size);

            with_file(open_data_file(meta_filename, rbuf), [&rbuf, &wbuf] (file& f) {
                return verify_data_file(f, rbuf, wbuf);
            }).get();
        });
    });
}
