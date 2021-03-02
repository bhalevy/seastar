#!/usr/bin/env python

import sys
import argparse
import re

import addr2line

parser = argparse.ArgumentParser(description='A backtrace tree.')
parser.add_argument('--max-address', metavar='max_address', type=str, default='0x100000000',
                    help='a maximum threshold for program addresses')
parser.add_argument('-p', '--processed', metavar='processed', type=bool, action=argparse.BooleanOptionalAction,
                    help='Input is already processed as [total, count, avg, adress, ...] rows')
parser.add_argument('-e', '--executable', metavar='executable', type=str,
                    help='Decoded addr2line using given executable')
parser.add_argument('file', metavar='file', type=str, nargs='?',
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

def process(t: int, trace: list[str], max_address: int):
    n = root
    for i in range(0, len(trace)):
        addr = trace[i]
        if int(addr, 0) >= max_address:
            break
        n = n.push(addr, t)

def reprocess(total: int, count: int, trace: list[str], max_address: int):
    n = root
    for i in range(0, len(trace)):
        addr = trace[i]
        if int(addr, 0) >= max_address:
            break
        n = n.pushn(addr, total, count)

def print_tree(n=root, indent=0, idx=0, rel=1.0):
    avg = round(n.total/n.count) if n.count else 0
    l = f"{' ' * indent}[#{idx} {round(100*rel)}%] addr={n.addr} total={n.total} count={n.count} avg={avg}"
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
    return s

def sort_backtraces(n: Node, trace = []):
    clist = n.sorted_children()
    # print(f"--> {n} {clist}")
    if not clist:
        s = f"total={n.total:<5} count={n.count:<5} avg={round(n.total/n.count):<3}"
        for addr in trace:
            s += f" {addr}"
        print(s)
        return
    for c in clist:
        sort_backtraces(c, [c.addr] + trace)

max_address = int(args.max_address, 0)

input = open(args.file) if args.file else sys.stdin
count = 0
if not args.processed:
    stall_pat = re.compile('Reactor stall')
    for s in input:
        if not stall_pat.search(s):
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
        process(t, trace, max_address)
else:
    for s in input:
        trace = s.split()
        total = int(trace[0])
        count = int(trace[1])
        avg = int(trace[2])
        trace = trace[3:]
        trace.reverse()
        reprocess(total, count, trace, max_address)

try:
    print_tree()
except BrokenPipeError:
    pass
