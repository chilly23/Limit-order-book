#!/usr/bin/env python3
"""
Simple Limit Order Book Simulator
- Limit orders (price + qty) are matched by price-time priority
- Market orders are matched against best prices until filled
- Price levels keep FIFO queues of orders
- Lightweight benchmarking included
"""
from collections import deque, defaultdict, namedtuple
import bisect
import random
import time
import uuid

Order = namedtuple("Order", ["id", "side", "price", "qty", "timestamp"])
Trade = namedtuple("Trade", ["buy_id", "sell_id", "price", "qty", "timestamp"])

class OrderBook:
    def __init__(self):
        # price -> deque[Order]
        self.bids = defaultdict(deque)   # buy side, keyed by price
        self.asks = defaultdict(deque)   # sell side, keyed by price

        # sorted price lists for quick best-price lookup
        # bids_prices sorted descending, asks_prices sorted ascending
        self.bids_prices = []   # descending maintained via insert with bisect on negative
        self.asks_prices = []   # ascending

        self.trades = []

    def _insert_price(self, price_list, price, descending=False):
        # insert price preserving sort order
        if descending:
            # maintain descending using negative key trick
            key = -price
            keys = [-p for p in price_list]
            i = bisect.bisect_left(keys, key)
            if i == len(price_list) or price_list[i] != price:
                price_list.insert(i, price)
        else:
            i = bisect.bisect_left(price_list, price)
            if i == len(price_list) or price_list[i] != price:
                price_list.insert(i, price)

    def _remove_price_if_empty(self, price_map, price_list, price):
        if not price_map[price]:
            del price_map[price]
            # remove from price_list
            i = bisect.bisect_left(price_list, price)
            if i < len(price_list) and price_list[i] == price:
                price_list.pop(i)

    def add_limit_order(self, side, price, qty, timestamp=None):
        timestamp = timestamp or time.time()
        oid = str(uuid.uuid4())[:8]
        order = Order(id=oid, side=side, price=price, qty=qty, timestamp=timestamp)
        if side == "buy":
            self._match_buy_limit(order)
            if order.qty > 0:
                # leftover goes to bids
                self.bids[price].append(order._replace(qty=order.qty))
                self._insert_price(self.bids_prices, price, descending=True)
        else:
            self._match_sell_limit(order)
            if order.qty > 0:
                self.asks[price].append(order._replace(qty=order.qty))
                self._insert_price(self.asks_prices, price, descending=False)
        return order.id

    def add_market_order(self, side, qty, timestamp=None):
        timestamp = timestamp or time.time()
        oid = str(uuid.uuid4())[:8]
        order = Order(id=oid, side=side, price=None, qty=qty, timestamp=timestamp)
        if side == "buy":
            self._match_buy_market(order)
        else:
            self._match_sell_market(order)
        return order.id

    # Matching helpers
    def _match_buy_limit(self, order):
        # Buy limit matches against best asks while ask_price <= order.price
        while order.qty > 0 and self.asks_prices and self.asks_prices[0] <= order.price:
            best_price = self.asks_prices[0]
            q_deque = self.asks[best_price]
            while q_deque and order.qty > 0:
                top = q_deque[0]
                trade_qty = min(order.qty, top.qty)
                # create trade: buy is incoming order, sell is top
                t = Trade(buy_id=order.id, sell_id=top.id, price=best_price, qty=trade_qty, timestamp=time.time())
                self.trades.append(t)
                order = order._replace(qty=order.qty - trade_qty)
                top = top._replace(qty=top.qty - trade_qty)
                q_deque[0] = top
                if top.qty == 0:
                    q_deque.popleft()
            self._remove_price_if_empty(self.asks, self.asks_prices, best_price)

    def _match_sell_limit(self, order):
        # Sell limit matches against best bids while bid_price >= order.price
        while order.qty > 0 and self.bids_prices and self.bids_prices[0] >= order.price:
            best_price = self.bids_prices[0]
            q_deque = self.bids[best_price]
            while q_deque and order.qty > 0:
                top = q_deque[0]
                trade_qty = min(order.qty, top.qty)
                t = Trade(buy_id=top.id, sell_id=order.id, price=best_price, qty=trade_qty, timestamp=time.time())
                self.trades.append(t)
                order = order._replace(qty=order.qty - trade_qty)
                top = top._replace(qty=top.qty - trade_qty)
                q_deque[0] = top
                if top.qty == 0:
                    q_deque.popleft()
            self._remove_price_if_empty(self.bids, self.bids_prices, best_price)

    def _match_buy_market(self, order):
        # Buy market: consume asks from cheapest upwards
        while order.qty > 0 and self.asks_prices:
            best_price = self.asks_prices[0]
            q_deque = self.asks[best_price]
            while q_deque and order.qty > 0:
                top = q_deque[0]
                trade_qty = min(order.qty, top.qty)
                t = Trade(buy_id=order.id, sell_id=top.id, price=best_price, qty=trade_qty, timestamp=time.time())
                self.trades.append(t)
                order = order._replace(qty=order.qty - trade_qty)
                top = top._replace(qty=top.qty - trade_qty)
                q_deque[0] = top
                if top.qty == 0:
                    q_deque.popleft()
            self._remove_price_if_empty(self.asks, self.asks_prices, best_price)
        # any remaining market qty that cannot be filled is ignored (simulate partial fill/no fill)

    def _match_sell_market(self, order):
        # Sell market: consume bids from highest downwards
        while order.qty > 0 and self.bids_prices:
            best_price = self.bids_prices[0]
            q_deque = self.bids[best_price]
            while q_deque and order.qty > 0:
                top = q_deque[0]
                trade_qty = min(order.qty, top.qty)
                t = Trade(buy_id=top.id, sell_id=order.id, price=best_price, qty=trade_qty, timestamp=time.time())
                self.trades.append(t)
                order = order._replace(qty=order.qty - trade_qty)
                top = top._replace(qty=top.qty - trade_qty)
                q_deque[0] = top
                if top.qty == 0:
                    q_deque.popleft()
            self._remove_price_if_empty(self.bids, self.bids_prices, best_price)

    # Utility / reporting
    def best_bid(self):
        return self.bids_prices[0] if self.bids_prices else None

    def best_ask(self):
        return self.asks_prices[0] if self.asks_prices else None

    def snapshot(self, depth=5):
        bids = [(p, sum(o.qty for o in self.bids[p])) for p in self.bids_prices[:depth]]
        asks = [(p, sum(o.qty for o in self.asks[p])) for p in self.asks_prices[:depth]]
        return {"bids": bids, "asks": asks}

    def clear_trades(self):
        self.trades = []

# Simple workload generator and benchmark
def random_price(base=100, spread=5):
    return round(base + random.randint(-spread, spread) + random.random(), 2)

def benchmark(order_count=10000, seed=42):
    random.seed(seed)
    ob = OrderBook()
    start = time.perf_counter()
    latencies = []
    for i in range(order_count):
        side = random.choice(["buy", "sell"])
        # prefer limit orders, occasional market orders
        if random.random() < 0.9:
            price = random_price(base=100, spread=10)
            qty = random.randint(1, 10)
            t0 = time.perf_counter()
            ob.add_limit_order(side, price, qty)
            t1 = time.perf_counter()
        else:
            qty = random.randint(1, 20)
            t0 = time.perf_counter()
            ob.add_market_order(side, qty)
            t1 = time.perf_counter()
        latencies.append((t1 - t0) * 1e6)  # microseconds
    total = time.perf_counter() - start
    avg_latency_us = sum(latencies) / len(latencies)
    tps = order_count / total
    print(f"Processed {order_count} orders in {total:.3f}s — {tps:.0f} orders/s, avg latency {avg_latency_us:.1f} us")
    print("Top of book:", ob.snapshot(3))
    return ob

if __name__ == "__main__":
    # quick demo
    ob = OrderBook()
    # seed with some limit liquidity
    for p in [99.5, 100.0, 100.5]:
        ob.add_limit_order("sell", price=p, qty=100)
        ob.add_limit_order("buy", price=p-1, qty=100)
    # place a buy market order
    ob.add_market_order("buy", qty=50)
    print("Trades after single market buy:", ob.trades)
    ob.clear_trades()
    # run benchmark
    benchmark(order_count=5000)
