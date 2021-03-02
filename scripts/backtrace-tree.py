#!/usr/bin/env python

import argparse
import sys
import re

import addr2line

parser = argparse.ArgumentParser(description='A backtrace tree analyser.')
parser.add_argument('--address-threshold', type=str, default='0x100000000',
                    help='Stop backtrace processing when reaching an address greater or equal to the threshold')
parser.add_argument('-r', '--reprocess', type=bool, action=argparse.BooleanOptionalAction,
                    help='Reprocess input rows formatted as "Backtrace [0-9]+: total=[0-9]+ count=[0-9]+ avg=[0-9]+: address ..."')
parser.add_argument('-e', '--executable', type=str,
                    help='Decode addresses to lines using given executable')
parser.add_argument('-o', '--output-type', type=str, choices=['backtraces', 'tree'], default='backtraces',
                    help='Select output type')
parser.add_argument('file', type=str, nargs='?',
                    help='an integer for the accumulator')

args = parser.parse_args()

resolver = addr2line.BacktraceResolver(executable=args.executable) if args.executable else None

class Node:
    def __init__(self, addr = "root"):
        self.addr = addr
        self.count = 0
        self.total = 0
        self.children = {}

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
            n = self.children[addr]
        except KeyError:
            n = Node(addr)
            self.children[addr] = n
        return n.add(t)

    def addn(self, total: int, count: int):
        self.count += count
        self.total += total
        return self

    def pushn(self, addr: str, total: int, count: int):
        try:
            n = self.children[addr]
        except KeyError:
            n = Node(addr)
            self.children[addr] = n
        return n.addn(total, count)

    def sorted_children(self, reverse=True):
        return sorted(list(self.children.values()), reverse=reverse)

root = Node()

def process(t: int, trace: list[str], address_threshold: int):
    n = root
    for i in range(0, len(trace)):
        addr = trace[i]
        if int(addr, 0) >= address_threshold:
            break
        n = n.push(addr, t)

def reprocess(total: int, count: int, trace: list[str], address_threshold: int):
    n = root
    for i in range(0, len(trace)):
        addr = trace[i]
        if int(addr, 0) >= address_threshold:
            break
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
    for c in n.sorted_children():
        print_tree(c, indent + 1, i, c.total / n.total if n.total else 1.0)
        i += 1

def print_tree_backtraces():
    def recursive_print_tree_backtraces(n, bt=[], desc=[], level=-1, idx=0, rel=1.0):
        nonlocal bt_num
        nonlocal line_sep
        if level >= 0:
            bt = [n.addr] + bt
            if resolver:
                desc = [ f"[{level:>2}#{idx:<2} {round(100*rel):>3}%] {resolver.resolve_address(n.addr)}" ] + desc

        children = n.sorted_children()
        if children:
            i = 0
            for c in children:
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
        trace.reverse()
        process(t, trace, address_threshold)
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
        trace.reverse()
        reprocess(total, count, trace, address_threshold)

try:
    if args.output_type == 'tree':
        print_tree()
    else:
        print_tree_backtraces()
except BrokenPipeError:
    pass
