from queue import CustomQueue
import time

q = CustomQueue(fifo=True)
q.push("a")
q.push("b")

assert q.pop() == "a"
assert q.pop() == "b"

q = CustomQueue(fifo=False)
q.push("a")
q.push("b")

assert q.pop() == "b"
assert q.pop() == "a"

q = CustomQueue(fifo=True)
q.push("low", priority=1)
q.push("high", priority=10)
q.push("normal")

assert q.pop() == "high"
assert q.pop() == "low"
assert q.pop() == "normal"

q = CustomQueue(fifo=True)
q.push('a', 1)
q.push('b', 100)
q.push('c', 10)

assert q.pop() == "b" # a (lowest priority)
assert q.pop() == "c" # b (highest priority)
assert q.pop() == "a" # c (medium priority)

q = CustomQueue(fifo=True)
q.push('a', delay=10)
q.push('b', delay=0)

assert q.pop() == "b"
time.sleep(10)
assert q.pop() == "a"

print("All tests passed")