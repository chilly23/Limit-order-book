#!/usr/bin/env python3

from collections import deque, defaultdict, namedtuple
import bisect
import random
import time
import uuid

order_t = namedtuple("order_t", ["oid", "side", "price", "qty", "ts"])
trade_t = namedtuple("trade_t", ["bid", "ask", "price", "qty", "ts"])


class lob:
    def __init__(self):
        self.bids = defaultdict(deque)
        self.asks = defaultdict(deque)

        self.bid_px = []
        self.ask_px = []

        self.trades = []

    def _add_px(self, lst, px, desc=False):
        if desc:
            neg = [-x for x in lst]
            idx = bisect.bisect_left(neg, -px)
            if idx == len(lst) or lst[idx] != px:
                lst.insert(idx, px)
        else:
            idx = bisect.bisect_left(lst, px)
            if idx == len(lst) or lst[idx] != px:
                lst.insert(idx, px)

    def _drop_px(self, mp, lst, px):
        if not mp[px]:
            del mp[px]
            i = bisect.bisect_left(lst, px)
            if i < len(lst) and lst[i] == px:
                lst.pop(i)

    def add_limit(self, side, px, qty, ts=None):
        ts = ts or time.time()
        oid = str(uuid.uuid4())[:8]
        o = order_t(oid, side, px, qty, ts)

        if side == "buy":
            o = self._hit_asks(o)
            if o.qty > 0:
                self.bids[px].append(o)
                self._add_px(self.bid_px, px, desc=True)
        else:
            o = self._hit_bids(o)
            if o.qty > 0:
                self.asks[px].append(o)
                self._add_px(self.ask_px, px)

        return oid

    def add_market(self, side, qty, ts=None):
        ts = ts or time.time()
        oid = str(uuid.uuid4())[:8]
        o = order_t(oid, side, None, qty, ts)
        if side == "buy":
            self._mkt_buy(o)
        else:
            self._mkt_sell(o)
        return oid

    # matching stuff

    def _hit_asks(self, o):
        while o.qty > 0 and self.ask_px and self.ask_px[0] <= o.price:
            px = self.ask_px[0]
            q = self.asks[px]
            while q and o.qty > 0:
                top = q[0]
                fill = min(o.qty, top.qty)
                t = trade_t(o.oid, top.oid, px, fill, time.time())
                self.trades.append(t)
                o = o._replace(qty=o.qty - fill)
                top = top._replace(qty=top.qty - fill)
                q[0] = top
                if top.qty == 0:
                    q.popleft()
            self._drop_px(self.asks, self.ask_px, px)
        return o

    def _hit_bids(self, o):
        while o.qty > 0 and self.bid_px and self.bid_px[0] >= o.price:
            px = self.bid_px[0]
            q = self.bids[px]
            while q and o.qty > 0:
                top = q[0]
                fill = min(o.qty, top.qty)
                t = trade_t(top.oid, o.oid, px, fill, time.time())
                self.trades.append(t)
                o = o._replace(qty=o.qty - fill)
                top = top._replace(qty=top.qty - fill)
                q[0] = top
                if top.qty == 0:
                    q.popleft()
            self._drop_px(self.bids, self.bid_px, px)
        return o

    def _mkt_buy(self, o):
        while o.qty > 0 and self.ask_px:
            px = self.ask_px[0]
            q = self.asks[px]
            while q and o.qty > 0:
                top = q[0]
                fill = min(o.qty, top.qty)
                t = trade_t(o.oid, top.oid, px, fill, time.time())
                self.trades.append(t)
                o = o._replace(qty=o.qty - fill)
                top = top._replace(qty=top.qty - fill)
                q[0] = top
                if top.qty == 0:
                    q.popleft()
            self._drop_px(self.asks, self.ask_px, px)

    def _mkt_sell(self, o):
        while o.qty > 0 and self.bid_px:
            px = self.bid_px[0]
            q = self.bids[px]
            while q and o.qty > 0:
                top = q[0]
                fill = min(o.qty, top.qty)
                t = trade_t(top.oid, o.oid, px, fill, time.time())
                self.trades.append(t)
                o = o._replace(qty=o.qty - fill)
                top = top._replace(qty=top.qty - fill)
                q[0] = top
                if top.qty == 0:
                    q.popleft()
            self._drop_px(self.bids, self.bid_px, px)

    # misc

    def best_bid(self):
        return self.bid_px[0] if self.bid_px else None

    def best_ask(self):
        return self.ask_px[0] if self.ask_px else None

    def snap(self, depth=5):
        b = [(p, sum(x.qty for x in self.bids[p])) for p in self.bid_px[:depth]]
        a = [(p, sum(x.qty for x in self.asks[p])) for p in self.ask_px[:depth]]
        return {"bids": b, "asks": a}

    def drop_trades(self):
        self.trades = []


# lazy benchmark

def rand_px(base=100, wiggle=5):
    return round(base + random.randint(-wiggle, wiggle) + random.random(), 2)


def bench(n=10000, seed=1):
    random.seed(seed)
    ob = lob()
    t0 = time.perf_counter()
    lat = []
    for _ in range(n):
        s = random.choice(["buy", "sell"])
        if random.random() < 0.9:
            px = rand_px(100, 10)
            q = random.randint(1, 10)
            t1 = time.perf_counter()
            ob.add_limit(s, px, q)
            t2 = time.perf_counter()
        else:
            q = random.randint(1, 20)
            t1 = time.perf_counter()
            ob.add_market(s, q)
            t2 = time.perf_counter()
        lat.append((t2 - t1) * 1e6)
    tot = time.perf_counter() - t0
    print("done", n, "orders in", round(tot, 3), "sec",
          "|", int(n / tot), "ops/s",
          "| avg", round(sum(lat) / len(lat), 2), "us")
    print("top:", ob.snap(3))
    return ob


if __name__ == "__main__":
    ob = lob()
    for p in [99.5, 100.0, 100.5]:
        ob.add_limit("sell", p, 100)
        ob.add_limit("buy", p - 1, 100)

    ob.add_market("buy", 50)
    print("trades:", ob.trades)
    ob.drop_trades()

    bench(5000)
