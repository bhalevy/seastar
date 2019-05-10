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
 * Copyright (C) 2016 ScyllaDB.
 */

#pragma once
#include <atomic>

namespace seastar {

namespace internal {

struct preemption_monitor {
    // We preempt when head != tail
    // This happens to match the Linux aio completion ring, so we can have the
    // kernel preempt a task by queuing a completion event to an io_context.
    std::atomic<uint32_t> head;
    std::atomic<uint32_t> tail;
};

}

bool need_preempt();

}
