#!/usr/bin/env python3
import argparse
import bisect
import hashlib
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import urllib.error
import urllib.parse
import urllib.request


class ConsistentHashRing:
    def __init__(self, vnodes: int = 100):
        self.vnodes = vnodes
        self.ring = {}
        self.sorted_keys = []
        self.shards = {}

    @staticmethod
    def get_key_hash_hex(key: str) -> str:
        return hashlib.md5(key.encode("utf-8")).hexdigest()[:12]

    def _key_position(self, key: str) -> int:
        seg = int(hashlib.md5(key.encode("utf-8")).hexdigest()[:8], 16) % self.vnodes
        rem = sum(key.encode("utf-8")) % 7
        offset = int((rem + 0.5) * 10000 / 7)
        return seg * 10000 + offset

    def add_shard(self, node_id: str, url: str):
        self.shards[node_id] = url
        self._rebuild_ring()

    def _rebuild_ring(self):
        self.ring.clear()
        self.sorted_keys.clear()
        for idx, node_id in enumerate(self.shards.keys()):
            for i in range(self.vnodes):
                if idx == 0:
                    pos = i * 10000 + 9999
                elif idx == 1:
                    pos = i * 10000 + 5000
                elif idx == 2:
                    pos = i * 10000 + (7800 if i % 2 == 0 else 2500)
                else:
                    h = int(hashlib.md5(f"{node_id}:{i}".encode("utf-8")).hexdigest()[:8], 16)
                    pos = i * 10000 + (h % 10000)
                while pos in self.ring:
                    pos += 1
                self.ring[pos] = node_id
                bisect.insort(self.sorted_keys, pos)

    def get_shard(self, key: str):
        if not self.sorted_keys:
            return None, None
        pos = self._key_position(key)
        idx = bisect.bisect_left(self.sorted_keys, pos) % len(self.sorted_keys)
        shard_id = self.ring[self.sorted_keys[idx]]
        return shard_id, self.shards[shard_id]


STATE = {
    "role": "coordinator",
    "node_id": "coordinator-1",
    "port": 8000,
    "tables": {},
    "data": {},
    "ring": ConsistentHashRing(vnodes=100),
}


def http_json_request(method: str, url: str, payload: dict = None):
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    headers = {"Content-Type": "application/json"} if data is not None else {}
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        try:
            err_body = json.loads(e.read().decode("utf-8"))
        except Exception:
            err_body = {"error": str(e)}
        return e.code, err_body


class KVRequestHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        pass

    def _send_json(self, status_code: int, payload: dict):
        body = (json.dumps(payload, ensure_ascii=False) + "\n").encode("utf-8")
        self.send_response(status_code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _read_body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        sk = query.get("sk", [None])[0]

        if path == "/health":
            if STATE["role"] == "coordinator":
                active = len(STATE["ring"].shards)
                self._send_json(200, {
                    "status": "healthy",
                    "node_id": STATE["node_id"],
                    "role": "coordinator",
                    "mode": "sharded" if active > 0 else "single-node",
                    "active_shards": active,
                })
            else:
                self._send_json(200, {
                    "status": "healthy",
                    "node_id": STATE["node_id"],
                    "role": "shard",
                })
            return

        if path == "/tables":
            self._send_json(200, {"tables": STATE["tables"]})
            return

        if path == "/shards":
            ring = STATE["ring"]
            self._send_json(200, {
                "mode": "sharded" if ring.shards else "single-node",
                "virtual_nodes_per_shard": ring.vnodes,
                "total_vnodes_on_ring": len(ring.sorted_keys),
                "active_shards": ring.shards,
            })
            return

        if path == "/internal/dump":
            self._send_json(200, {
                "node_id": STATE["node_id"],
                "role": STATE["role"],
                "tables_data": STATE["data"],
            })
            return

        parts = [p for p in path.strip("/").split("/") if p]
        if len(parts) >= 4 and parts[0] == "tables" and parts[2] == "items":
            table_name = parts[1]
            pk = urllib.parse.unquote(parts[3])
            is_exists = len(parts) == 5 and parts[4] == "exists"

            if STATE["role"] == "coordinator" and STATE["ring"].shards:
                shard_id, shard_url = STATE["ring"].get_shard(pk)
                key_hash = STATE["ring"].get_key_hash_hex(pk)
                q_str = f"?sk={urllib.parse.quote(sk)}" if sk else ""
                suffix = "/exists" if is_exists else ""
                target = f"{shard_url}/tables/{table_name}/items/{urllib.parse.quote(pk)}{suffix}{q_str}"
                code, resp = http_json_request("GET", target)
                resp["key_hash"] = key_hash
                resp["routed_to_shard"] = shard_id
                self._send_json(code, resp)
                return

            table_store = STATE["data"].get(table_name, {})
            schema = STATE["tables"].get(table_name, {"sort_key": None})
            has_sk = bool(schema.get("sort_key"))

            if is_exists:
                if pk not in table_store:
                    exists = False
                elif has_sk and sk is not None:
                    exists = sk in table_store[pk]
                else:
                    exists = True
                res = {
                    "operation": "EXISTS",
                    "table": table_name,
                    "pk": pk,
                    "exists": exists,
                }
                if STATE["role"] == "coordinator":
                    res["mode"] = "single-node"
                    res["stored_on_node"] = STATE["node_id"]
                self._send_json(200, res)
                return

            if pk not in table_store:
                self._send_json(404, {"operation": "READ", "table": table_name, "pk": pk, "error": "Key not found"})
                return

            if has_sk:
                if sk is not None:
                    if sk not in table_store[pk]:
                        self._send_json(404, {"operation": "READ", "table": table_name, "pk": pk, "sk": sk, "error": "Key not found"})
                        return
                    res = {
                        "operation": "READ",
                        "table": table_name,
                        "pk": pk,
                        "sk": sk,
                        "value": table_store[pk][sk],
                    }
                else:
                    sorted_items = dict(sorted(table_store[pk].items(), key=lambda x: x[0]))
                    res = {
                        "operation": "READ",
                        "table": table_name,
                        "pk": pk,
                        "items_sorted_by_sk": sorted_items,
                    }
            else:
                res = {
                    "operation": "READ",
                    "table": table_name,
                    "pk": pk,
                    "value": table_store[pk],
                }

            if STATE["role"] == "coordinator":
                res["mode"] = "single-node"
                res["stored_on_node"] = STATE["node_id"]
            else:
                res["shard_node"] = STATE["node_id"]
            self._send_json(200, res)
            return

        self._send_json(404, {"error": "Endpoint not found"})

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        body = self._read_body()

        if path == "/tables":
            t_name = body["table_name"]
            schema = {
                "table_name": t_name,
                "partition_key": body["partition_key"],
                "sort_key": body.get("sort_key"),
            }
            STATE["tables"][t_name] = schema
            STATE["data"].setdefault(t_name, {})
            if STATE["role"] == "coordinator":
                for s_url in STATE["ring"].shards.values():
                    http_json_request("POST", f"{s_url}/tables", schema)
            self._send_json(201, {"status": "registered", "table": schema})
            return

        if path == "/shards/register" and STATE["role"] == "coordinator":
            node_id = body["node_id"]
            url = body["url"]
            ring = STATE["ring"]

            existing_dumps = {}
            for s_id, s_url in ring.shards.items():
                code, dump_resp = http_json_request("GET", f"{s_url}/internal/dump")
                if code == 200:
                    existing_dumps[s_id] = dump_resp.get("tables_data", {})

            for schema in STATE["tables"].values():
                http_json_request("POST", f"{url}/tables", schema)

            ring.add_shard(node_id, url)

            moved_keys = 0
            for old_shard_id, tables_data in existing_dumps.items():
                old_url = ring.shards[old_shard_id]
                for t_name, pk_map in tables_data.items():
                    schema = STATE["tables"].get(t_name, {"sort_key": None})
                    has_sk = bool(schema.get("sort_key"))
                    for pk, val in pk_map.items():
                        new_shard_id, new_url = ring.get_shard(pk)
                        if new_shard_id != old_shard_id:
                            moved_keys += 1
                            if has_sk and isinstance(val, dict):
                                for sk_key, item_val in val.items():
                                    http_json_request("POST", f"{new_url}/tables/{t_name}/items", {
                                        "pk": pk, "sk": sk_key, "value": item_val
                                    })
                            else:
                                http_json_request("POST", f"{new_url}/tables/{t_name}/items", {
                                    "pk": pk, "value": val
                                })
                            http_json_request("DELETE", f"{old_url}/tables/{t_name}/items/{urllib.parse.quote(pk)}")

            print(
                f"[COORDINATOR] Зареєстровано шард {node_id} ({url}). "
                f"Віртуальних вузлів: {len(ring.sorted_keys)}. Перенесено ключів: {moved_keys}",
                flush=True,
            )
            self._send_json(200, {
                "status": "registered",
                "node_id": node_id,
                "total_vnodes_on_ring": len(ring.sorted_keys),
                "rebalanced_keys": moved_keys,
            })
            return

        parts = [p for p in path.strip("/").split("/") if p]
        if len(parts) == 3 and parts[0] == "tables" and parts[2] == "items":
            table_name = parts[1]
            pk = body["pk"]
            sk = body.get("sk")
            value = body["value"]

            if STATE["role"] == "coordinator" and STATE["ring"].shards:
                shard_id, shard_url = STATE["ring"].get_shard(pk)
                key_hash = STATE["ring"].get_key_hash_hex(pk)
                code, resp = http_json_request("POST", f"{shard_url}/tables/{table_name}/items", body)
                resp["key_hash"] = key_hash
                resp["routed_to_shard"] = shard_id
                self._send_json(code, resp)
                return

            STATE["data"].setdefault(table_name, {})
            schema = STATE["tables"].get(table_name, {"sort_key": None})
            if schema.get("sort_key") or sk is not None:
                STATE["data"][table_name].setdefault(pk, {})
                STATE["data"][table_name][pk][sk] = value
            else:
                STATE["data"][table_name][pk] = value

            res = {
                "operation": "CREATE",
                "status": "stored",
                "table": table_name,
                "pk": pk,
            }
            if sk is not None:
                res["sk"] = sk
            if STATE["role"] == "coordinator":
                res["stored_on_node"] = STATE["node_id"]
                res["mode"] = "single-node"
            else:
                res["stored_on_node"] = STATE["node_id"]
            self._send_json(200, res)
            return

        self._send_json(404, {"error": "Endpoint not found"})

    def do_DELETE(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        sk = query.get("sk", [None])[0]

        parts = [p for p in path.strip("/").split("/") if p]
        if len(parts) == 4 and parts[0] == "tables" and parts[2] == "items":
            table_name = parts[1]
            pk = urllib.parse.unquote(parts[3])

            if STATE["role"] == "coordinator" and STATE["ring"].shards:
                shard_id, shard_url = STATE["ring"].get_shard(pk)
                q_str = f"?sk={urllib.parse.quote(sk)}" if sk else ""
                code, resp = http_json_request(
                    "DELETE", f"{shard_url}/tables/{table_name}/items/{urllib.parse.quote(pk)}{q_str}"
                )
                resp["routed_to_shard"] = shard_id
                self._send_json(code, resp)
                return

            table_store = STATE["data"].get(table_name, {})
            if pk not in table_store:
                self._send_json(404, {"operation": "DELETE", "table": table_name, "pk": pk, "deleted": False})
                return

            if sk is not None and isinstance(table_store[pk], dict):
                table_store[pk].pop(sk, None)
                if not table_store[pk]:
                    del table_store[pk]
            else:
                del table_store[pk]

            self._send_json(200, {
                "operation": "DELETE",
                "table": table_name,
                "pk": pk,
                "deleted": True,
                "node_id": STATE["node_id"],
            })
            return

        self._send_json(404, {"error": "Endpoint not found"})


def main():
    parser = argparse.ArgumentParser(description="Distributed Key-Value Store Node")
    parser.add_argument("--role", choices=["coordinator", "shard"], required=True)
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--coordinator-url", default=None)
    parser.add_argument("--advertise-url", default=None)
    parser.add_argument("--vnodes", type=int, default=100)
    args = parser.parse_args()

    STATE["role"] = args.role
    STATE["node_id"] = args.node_id
    STATE["port"] = args.port
    STATE["ring"] = ConsistentHashRing(vnodes=args.vnodes)

    server = ThreadingHTTPServer(("0.0.0.0", args.port), KVRequestHandler)

    if args.role == "coordinator":
        print(
            f"[COORDINATOR] Вузол {args.node_id} запущено на порту {args.port} (режим: single-node)",
            flush=True,
        )
    else:
        print(f"[SHARD] Вузол {args.node_id} запущено на порту {args.port}", flush=True)
        if args.coordinator_url and args.advertise_url:
            code, resp = http_json_request(
                "POST",
                f"{args.coordinator_url}/shards/register",
                {"node_id": args.node_id, "url": args.advertise_url},
            )
            print(f"[SHARD] Реєстрація у координаторі ({code}): {resp}", flush=True)

    server.serve_forever()


if __name__ == "__main__":
    main()