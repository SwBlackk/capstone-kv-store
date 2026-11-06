#!/usr/bin/env python3
import bisect
import hashlib
from collections import Counter


def hash_int(text: str) -> int:
    return int(hashlib.md5(text.encode("utf-8")).hexdigest()[:8], 16)


class ConsistentHashRing:
    def __init__(self, nodes, vnodes: int = 100):
        self.vnodes = vnodes
        self.ring = {}
        self.sorted_keys = []
        for node in nodes:
            self.add_node(node)

    def add_node(self, node: str):
        for i in range(self.vnodes):
            pos = hash_int(f"{node}#vnode#{i}")
            while pos in self.ring:
                pos += 1
            self.ring[pos] = node
            bisect.insort(self.sorted_keys, pos)

    def get_node(self, key: str) -> str:
        h = hash_int(key)
        idx = bisect.bisect_left(self.sorted_keys, h) % len(self.sorted_keys)
        return self.ring[self.sorted_keys[idx]]


def main():
    total_keys = 10000
    keys = [f"user_key_{i}" for i in range(total_keys)]
    shards_2 = ["shard-1", "shard-2"]
    shards_3 = ["shard-1", "shard-2", "shard-3"]

    mod_2 = {k: shards_2[hash_int(k) % 2] for k in keys}
    mod_3 = {k: shards_3[hash_int(k) % 3] for k in keys}
    moved_mod = sum(1 for k in keys if mod_2[k] != mod_3[k])

    ring_2 = ConsistentHashRing(shards_2, vnodes=100)
    ch_2 = {k: ring_2.get_node(k) for k in keys}
    dist_2 = Counter(ch_2.values())

    ring_3 = ConsistentHashRing(shards_3, vnodes=100)
    ch_3 = {k: ring_3.get_node(k) for k in keys}
    dist_3 = Counter(ch_3.values())
    moved_ch = sum(1 for k in keys if ch_2[k] != ch_3[k])

    print("=" * 68)
    print(f"ЕКСПЕРИМЕНТ: Масштабування кластера з 2 до 3 шардів ({total_keys} ключів)")
    print("=" * 68)
    print(f"1. Звичайне модульне хешування (hash % N):")
    print(f"   - Переміщено ключів (2 -> 3 шарди): {moved_mod} з {total_keys} ({moved_mod / total_keys * 100:.2f}%)")
    print("-" * 68)
    print(f"2. Консистентне хешування (Consistent Hashing, 100 vnodes/shard):")
    print(f"   - Розподіл на 2 шардах: {dict(dist_2)}")
    print(f"   - Розподіл на 3 шардах: {dict(dist_3)}")
    print(f"   - Переміщено ключів (2 -> 3 шарди): {moved_ch} з {total_keys} ({moved_ch / total_keys * 100:.2f}%)")
    print("=" * 68)


if __name__ == "__main__":
    main()