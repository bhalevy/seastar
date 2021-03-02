#!/usr/bin/env python

import argparse
import sys
import re

import addr2line

parser = argparse.ArgumentParser(formatter_class=argparse.RawDescriptionHelpFormatter,
    description='A backtrace tree analyser.',
    epilog="""
Backtrace-tree analyzes a series of reactor-stall backtraces using a tree.
Each node in the tree holds:
  `addr` - a program address
  `count` - number of backtraces going through the address
  `total` - the total sum of stalls, in milliseconds
            of all reactor stalls that mention the address.

When printed, the tree is traversed in descending `total` order
so that we print stall backtraces that are frequent and long.

Backtraces are enumerated in tree-traversal order.

When decoding the addresses given an executable, each line in
the decoded backtrace is preceded with [level#index pct%],
where `level` indicates the level of the node in the tree,
similar to gdb frame identifiers, `index` is the node's index
in the sorted callers list of that node, and `pct` is the
node's `total` weight relative to the parent`total`,
which is equal to the sum of all its callers's
total stall time.
""")
parser.add_argument('--address-threshold', default='0x100000000',
                    help='Stop backtrace processing when reaching an address greater or equal to the threshold (0 to disable)')
parser.add_argument('-r', '--reprocess', action='store_const', const=True, default=False,
                    help='Reprocess self\'s output. Input format is expected to be "Backtrace [0-9]+: total=[0-9]+ count=[0-9]+ avg=[0-9]+: address ..."')
parser.add_argument('-e', '--executable',
                    help='Decode addresses to lines using given executable')
parser.add_argument('-o', '--output-type', choices=['backtraces', 'tree'], default='backtraces',
                    help='Select output type')
parser.add_argument('file', nargs='?',
                    help='File containing reactor stall backtraces')

args = parser.parse_args()

resolver = addr2line.BacktraceResolver(executable=args.executable) if args.executable else None

class Node:
    def __init__(self, addr = "root"):
        # Each node in the tree contains:
        self.addr = addr    # Program address
        self.count = 0      # Number of backtraces going through the address
                            # Equal to the sum of callers counts for non-leaf nodes.
        self.total = 0      # Total stall time in milliseconds of all backtraces going through this node
                            # Equal to the sum of callers totals for non-leaf nodes.
        self.callers = {}  # descendants of this node.

    def __eq__(self, other):
        return self.total == other.total and self.count == other.count

    def __ne__(self, other):
        return not (self == other)

    def __lt__(self, other):
        return self.total < other.total or (self.total == other.total and self.count <= other.count)

    def __repr__(self):
        return f"Node({self.addr}: total={self.total}, count={self.count}, avg={round(self.total/self.count) if self.count else 0})"

    def add(self, t: int):
        self.count += 1
        self.total += t
        return self

    def push(self, addr: str, t: int):
        try:
            n = self.callers[addr]
        except KeyError:
            n = Node(addr)
            self.callers[addr] = n
        return n.add(t)

    def addn(self, total: int, count: int):
        self.count += count
        self.total += total
        return self

    def pushn(self, addr: str, total: int, count: int):
        try:
            n = self.callers[addr]
        except KeyError:
            n = Node(addr)
            self.callers[addr] = n
        return n.addn(total, count)

    def sorted_callers(self, descending=True):
        return sorted(list(self.callers.values()), reverse=descending)

root = Node()

# process each backtrace and insert it to the tree
#
# The backtraces are assumed to be in bottom-up order, i.e.
# the first address indicates the innermost frame and the last
# address is in the outermost, in calling order.
#
# This helps identifying closely related reactor stalls
# where a code path that stalls may be called from multiple
# call sites.
def process(t: int, trace: list[str]):
    n = root
    for i in range(0, len(trace)):
        addr = trace[i]
        n = n.push(addr, t)

# reprocess backtraces and insert them to the tree
# this works on previously processed backtraces, that
# are now accompanied by `total` and `count` values.
def reprocess(total: int, count: int, trace: list[str]):
    n = root
    for i in range(0, len(trace)):
        addr = trace[i]
        n = n.pushn(addr, total, count)

def print_tree(n=root, indent=0, idx=0, rel=1.0):
    avg = round(n.total/n.count) if n.count else 0
    l = f"{' ' * indent}[#{idx} {round(100*rel)}%] (addr={n.addr} total={n.total} count={n.count} avg={avg})"
    cont = []
    if resolver and n.addr != "root":
        lines = resolver.resolve_address(n.addr).splitlines()
        l += ': ' + lines[0]
        cont = lines[1:]
    print(f"{l}")
    for l in cont:
        print(f"{' ' * indent}   {l}")
    i = 0
    for c in n.sorted_callers():
        print_tree(c, indent + 1, i, c.total / n.total if n.total else 1.0)
        i += 1

def print_tree_backtraces():
    def recursive_print_tree_backtraces(n, bt=[], desc=[], level=-1, idx=0, rel=1.0):
        nonlocal bt_num
        nonlocal line_sep
        if level >= 0:
            bt = bt + [n.addr]
            if resolver:
                node_desc = f"[{level:>2}#{idx:<2} {round(100*rel):>3}%] {resolver.resolve_address(n.addr)}"
                desc = desc + [node_desc]

        callers = n.sorted_callers()
        if callers:
            i = 0
            for c in callers:
                recursive_print_tree_backtraces(c, bt, desc, level + 1, i, c.total / n.total if n.total else 1.0)
                i += 1
        else:
            avg = round(n.total/n.count) if n.count else 0
            sys.stdout.write(f"{line_sep}Backtrace {bt_num}: total={n.total} count={n.count} avg={avg}: {' '.join(bt)}\n")
            bt_num += 1
            for t in desc:
                sys.stdout.write(t)
    bt_num = 0
    line_sep = '\n' if resolver else ''
    recursive_print_tree_backtraces(root)

address_threshold = int(args.address_threshold, 0)

input = open(args.file) if args.file else sys.stdin
count = 0
if not args.reprocess:
    pattern = re.compile('Reactor stall')
    for s in input:
        if not pattern.search(s):
            continue
        count += 1
        trace = s.split()
        for i in range(0, len(trace)):
            if trace[i] == 'Reactor':
                i += 3
                break
        t = int(trace[i])
        trace = trace[i + 6:]
        # The address_threshold typically indicates a library call
        # and the backtrace up-to and including it are usually of
        # no interest as they all contain the stall backtrace geneneration code, e.g.:
        #  seastar::internal::cpu_stall_detector::generate_trace
        # void seastar::backtrace<seastar::backtrace_buffer::append_backtrace_oneline()::{lambda(seastar::frame)#1}>(seastar::backt>
        #  (inlined by) seastar::backtrace_buffer::append_backtrace_oneline() at ./build/release/seastar/./seastar/src/core/reactor.cc:771
        #  (inlined by) seastar::print_with_backtrace(seastar::backtrace_buffer&, bool) at ./build/release/seastar/./seastar/src/core/reactor.cc>
        # seastar::internal::cpu_stall_detector::generate_trace() at ./build/release/seastar/./seastar/src/core/reactor.cc:1257
        # seastar::internal::cpu_stall_detector::maybe_report() at ./build/release/seastar/./seastar/src/core/reactor.cc:1103
        #  (inlined by) seastar::internal::cpu_stall_detector::on_signal() at ./build/release/seastar/./seastar/src/core/reactor.cc:1117
        #  (inlined by) seastar::reactor::block_notifier(int) at ./build/release/seastar/./seastar/src/core/reactor.cc:1240
        # ?? ??:0
        if address_threshold:
            for i in range(0, len(trace)):
                if int(trace[i], 0) >= address_threshold:
                    trace = trace[i+1:]
                    break
        process(t, trace)
else:
    pattern = re.compile('Backtrace')
    md_pattern = re.compile('(total|count|avg)=(\d+):?')
    for s in input:
        if not pattern.search(s):
            continue
        trace = s.split()
        for i in range(0, len(trace)):
            if trace[i].startswith("total="):
                total = int(re.match(md_pattern, trace[i]).group(2))
                count = int(re.match(md_pattern, trace[i+1]).group(2))
                avg = int(re.match(md_pattern, trace[i+2]).group(2))
                trace = trace[i+3:]
                break
        else:
            raise RuntimeError(f"Could not find 'total=' field: line='{s}'")
        reprocess(total, count, trace)

try:
    if args.output_type == 'tree':
        print_tree()
    else:
        print_tree_backtraces()
except BrokenPipeError:
    pass
