import heapq
import json
import threading
import time
from collections import deque

EPSILSON = 0.1  # 100ms

class _StackNode:
	def __init__(self, item, next_node=None):
		self.item = item
		self.next = next_node


class Item:
	def __init__(self, value, delay=0):
		self.value = value
		self.timestamp = time.time() + delay


class CustomQueue:
	def __init__(self, fifo: bool, wal_path: str = "queue_wal.log"):
		'''
		I added a mutex lock to simulate atomicity in case of concurrent access, which can cause problems.
		For example, two .pop() operations might both return value A, despite removing both value A and B.
		Or two .pop() operations might do a duplicate job in popping value A, even if the user expects the CustomQueue to be 2 items shorter.

		To implement persistence, I would use a write-ahead log (WAL) file in `queue_wal.log` to record all operations (push and pop) with timestamps.
		If the system crashes in the middle of an operation, we can reference the WAL to replay any failed operations and restore the queue to a consistent state.
		'''
		self.fifo = fifo
		self._ready_fifo = deque()  # to handle FIFO items with O(1) push and pop.
		self.stack = None  # to handle LIFO items with O(1) push and pop.
		self._priority_heap = []  # max-priority heap using negative priority: (-priority, sequence, item), with O(log n) push and pop.
		self._delayed_heap = []  # Min-timestamp heap of delayed FIFO items: (timestamp, sequence, item), with O(log n) push and pop.
		self._sequence = 0
		self._lock = threading.Lock() # mutex lock to handle concurrency
		self._wal_path = wal_path # write-ahead log for persistence

	def _append_wal(self, operation: str, **fields):
		entry = {
			"ts": time.time(),
			"operation": operation,
			**fields,
		}
		with open(self._wal_path, "a", encoding="utf-8") as wal_file:
			wal_file.write(json.dumps(entry, default=str) + "\n")

	def push(self, value, priority=None, delay=0):
		'''
		self._sequence is used to maintain insertion order for items with the same priority or timestamp.
		'''
		with self._lock:
			self._append_wal("push", value=value, priority=priority, delay=delay)
			item = Item(value, delay=delay)

			if priority is not None:
				heapq.heappush(self._priority_heap, (-priority, self._sequence, item))
				self._sequence += 1
				return

			if self.fifo:
				if delay > 0:
					heapq.heappush(self._delayed_heap, (item.timestamp, self._sequence, item))
					self._sequence += 1
					return

				self._ready_fifo.append(item)
				return

			self.stack = _StackNode(item, self.stack)

	def pop(self):
		with self._lock:
			self._append_wal("pop")
			now_plus_epsilon = time.time() + EPSILSON

			if self._priority_heap:
				_, _, item = self._priority_heap[0]
				if item.timestamp <= now_plus_epsilon:
					_, _, ready_item = heapq.heappop(self._priority_heap)
					return ready_item.value

			if self.fifo:
				while self._delayed_heap and self._delayed_heap[0][0] <= now_plus_epsilon:
					_, _, ready_item = heapq.heappop(self._delayed_heap)
					self._ready_fifo.append(ready_item)

				if self._ready_fifo:
					value = self._ready_fifo.popleft().value
					return value

				raise IndexError("pop from empty CustomQueue")

			if self.stack is not None and self.stack.item.timestamp <= now_plus_epsilon:
				node = self.stack
				self.stack = node.next
				return node.item.value

			raise IndexError("pop from empty CustomQueue")