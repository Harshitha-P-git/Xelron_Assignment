# Part 3: Prompt Preparation

## Selected PR: #196 — Added Separate Socket Groups to Client
**Repository:** `aio-libs/aiokafka`  
**PR URL:** https://github.com/aio-libs/aiokafka/pull/196

---

## 3.1.1 Repository Context

`aiokafka` is a Python library that gives developers a clean, native asyncio interface for interacting with Apache Kafka, the widely-used distributed message streaming platform. Kafka is fundamentally a system for publishing messages to topics and subscribing to consume those messages at scale, commonly used as a backbone for event-driven architectures, real-time data pipelines, and microservice communication.

What makes aiokafka special is that it does not block. Traditional Kafka client libraries for Python (like `kafka-python`) work synchronously — calling `consumer.poll()` will pause your program while it waits for messages. In an async Python application built with `asyncio`, this kind of blocking is unacceptable because it would freeze the entire event loop. aiokafka solves this by reimplementing the Kafka client protocol using Python's `async/await` system, letting your program do other work while waiting for Kafka responses.

The intended users are Python backend developers and platform engineers building high-throughput systems: real-time analytics engines, notification pipelines, financial transaction processors, or any service where messages must be consumed and produced without compromising application responsiveness. The library is part of the `aio-libs` organization, a community that maintains async-first Python libraries.

The problem domain is distributed systems networking. At its core, the library (rooted in [`aiokafka/`](https://github.com/aio-libs/aiokafka/tree/master/aiokafka)) handles TCP socket connections to Kafka broker nodes, negotiates the binary Kafka protocol, manages consumer group membership (heartbeats, offset commits, partition assignment), and guarantees reliable and ordered message delivery. These are non-trivial concerns — the library must handle network partitions, leader elections, broker failover, and consumer rebalances, all without disrupting the asyncio event loop.

---

## 3.1.2 Pull Request Description

Before PR #196, the [`AIOKafkaClient`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/client.py) maintained exactly one TCP socket connection per Kafka broker node. This seems reasonable at first — one connection per broker, keep it simple. But Kafka's binary protocol is synchronous on a given connection: you send a request, you wait for the response, then you send the next one. This means a single slow request can block all others going to the same broker.

The specific problem: a long-polling fetch request — a request that sits open on the broker for up to 500ms waiting for new messages to arrive — would block the socket entirely. Any other requests destined for that same broker node during those 500ms had to wait in a queue. Crucially, offset commits are sent to the Group Coordinator, which is often the same broker node as the partition leader. So a consumer that was actively fetching messages would block its own ability to commit offsets. Delayed offset commits meant missed heartbeat windows, which could trigger unnecessary consumer group rebalances — an expensive, disruptive operation for all consumers in the group.

This PR introduces "socket groups" — a concept where the client can maintain multiple TCP connections to the same broker node, each serving a different purpose. Specifically, it adds a `COORDINATION` socket group reserved for all group coordination traffic (offset commits, heartbeats, join/leave group requests). The default group continues to handle fetch and produce requests. The changes are contained in [`client.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/client.py) and [`group_coordinator.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/coordinator/group_coordinator.py).

**Previous behavior:** All requests to a broker node share one socket; coordination requests can be delayed by fetch requests.  
**New behavior:** Coordination requests travel on a dedicated socket that is never blocked by fetch operations.

---

## 3.1.3 Acceptance Criteria

✓ When a long-polling fetch request is in flight to a broker node, `GroupCoordinator` offset commit requests to the same node must be dispatched immediately without waiting for the fetch to complete.

✓ The implementation must create a separate TCP socket connection to a broker node specifically for the `COORDINATION` socket group, distinct from the connection used for fetch/produce requests.

✓ When `GroupCoordinator` sends any request (heartbeat, commit offsets, join group, leave group, sync group), it must specify the `COORDINATION` group so the request is routed through the dedicated socket.

✓ The `AIOKafkaClient` must accept a `group` parameter (or equivalent) on its request-sending methods to select the appropriate socket group, keying connections on `(node_id, group_name)` tuples.

✓ The implementation must handle the case where a `COORDINATION` group connection is not yet established — it must establish the connection on first use rather than raising an error.

✓ Existing tests for the consumer group coordinator must continue to pass without modification after the change.

✓ Unit tests must verify that requests destined for the `COORDINATION` group are sent over a different socket than requests using the default group, even when both target the same broker node.

✓ The change must fix [issue #137](https://github.com/aio-libs/aiokafka/issues/137) ("Consumer and Coordinator should use a separate socket") and [issue #128](https://github.com/aio-libs/aiokafka/issues/128) ("Slow commit").

✓ Code coverage of the modified files ([`client.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/client.py), [`group_coordinator.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/coordinator/group_coordinator.py)) must not drop below their pre-PR coverage levels.

✓ The default behavior for producers and consumers that do not use consumer groups must remain unchanged — they must continue to use a single socket per broker as before, with no additional resource overhead.

---

## 3.1.4 Edge Cases

**Edge Case 1: Coordinator and partition leader on the same broker node**  
This is the most common scenario that triggers the original bug. The `COORDINATION` group socket must be established independently from the fetch socket to the same host:port. The implementation must correctly distinguish connections to the same broker by group name and not just by node ID or address. If the coordinator migrates to a different broker mid-session (due to a Kafka leader election), the new coordinator connection must also use the `COORDINATION` group on the new target node, and the old coordinator connection must be cleaned up properly.

**Edge Case 2: Connection failure of the coordination socket**  
If the dedicated coordination socket drops (network blip, broker restart), the error must be handled gracefully and independently. The coordinator must detect the disconnection, attempt reconnection specifically via the `COORDINATION` group, and never fall back silently to the default group socket. Exponential backoff and retry logic must apply to the coordination socket independently of the data socket. A failed coordination socket must not invalidate or close the data fetch socket to the same broker.

**Edge Case 3: Consumer without a group_id (standalone consumer)**  
A consumer created without a `group_id` does not participate in group coordination — there is no heartbeating, no coordinator-based offset commits, and no rebalance protocol. In this scenario, the `COORDINATION` socket group must never be instantiated. The implementation must conditionally create the coordination connection only when a `GroupCoordinator` is active (i.e., a `group_id` is configured), ensuring no unnecessary resource overhead or idle connection for standalone consumers.

**Edge Case 4: High-frequency coordination requests and socket contention**  
If heartbeat and offset commit frequency is very high, the coordination socket could theoretically become a bottleneck. However, since coordination traffic consists of small, prompt-response messages (unlike the potentially large payload of fetch responses), the coordination socket must not be configured with a long `max_wait_ms` setting. The implementation must ensure that the `max_wait_ms` applied to the coordination group socket remains low, matching the responsiveness requirements of heartbeats and commits rather than the throughput optimization of fetches.

---

## 3.1.5 Initial Prompt

You are implementing a fix for a networking bottleneck in `aiokafka`, an open-source Python asyncio library that provides a non-blocking client for Apache Kafka. The repository is at https://github.com/aio-libs/aiokafka.

**Background:**

The Kafka binary protocol is synchronous on a single TCP connection — only one in-flight request is allowed per connection at a time. Currently, [`AIOKafkaClient`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/client.py) maintains one socket per Kafka broker node. A consumer that is long-polling for messages (waiting up to 500ms for new data from the same broker) holds that socket for the full duration. The [`GroupCoordinator`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/coordinator/group_coordinator.py), which handles offset commits, heartbeats, and group membership, often communicates with the same broker node. This means group coordination requests are delayed behind fetch requests, causing slow commits and potentially triggering unnecessary consumer rebalances. This bug is tracked in [issue #137](https://github.com/aio-libs/aiokafka/issues/137) and [issue #128](https://github.com/aio-libs/aiokafka/issues/128).

**What you need to implement:**

Implement the changes introduced by [PR #196](https://github.com/aio-libs/aiokafka/pull/196), which introduces "socket groups" into `AIOKafkaClient`. The core idea is:

1. The client must be able to maintain **multiple TCP connections to the same broker node**, each tagged with a named group.
2. Define at minimum two groups: a **default group** (used for fetch, produce, and metadata requests) and a **`COORDINATION` group** (used exclusively for group coordinator traffic).
3. The [`GroupCoordinator`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/coordinator/group_coordinator.py) class must be updated to specify the `COORDINATION` group when sending any of its requests: commit offsets, heartbeat, join group, leave group, sync group.
4. The client's request-sending interface must accept an optional `group` parameter. Requests sharing the same `(node_id, group)` key share one socket; different groups to the same node use separate, independent sockets.

**Files to modify:**

- [`aiokafka/client.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/client.py) — Extend connection management so the internal connection map keys on `(node_id, group_name)` instead of just `node_id`. Add a `group` keyword argument to the method(s) responsible for dispatching requests to broker nodes. Ensure the default group is used by all existing callers unless explicitly overridden.
- [`aiokafka/coordinator/group_coordinator.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/coordinator/group_coordinator.py) — Update all places where requests are sent to the coordinator node to pass the `COORDINATION` group constant. Do not miss any request type: heartbeat, offset commit, join group, leave group, sync group.

**Acceptance criteria to satisfy:**

- Fetch requests and offset commit requests to the same broker node must travel over different TCP sockets.
- `GroupCoordinator` requests must use the `COORDINATION` group; all other traffic must use the default group.
- A connection to the `COORDINATION` group socket must only be created when a `GroupCoordinator` is active (i.e., `group_id` is configured). Standalone consumers must not create a coordination socket.
- All existing tests must pass.
- Write at least one new test that verifies two simultaneous requests to the same broker node travel over different sockets when different groups are specified.

**Edge cases to handle:**

- The coordination socket may drop independently of the fetch socket — handle reconnection for the coordination group without affecting other groups' connections to the same broker.
- A standalone consumer (no `group_id`) must never instantiate a `COORDINATION` group socket.
- If the Kafka group coordinator migrates to a different broker during operation, the new coordinator target must also use the `COORDINATION` group, and the old coordination connection must be released.
- The coordinator socket must not inherit the long `max_wait_ms` timeout used by fetch requests — coordination traffic requires prompt, low-latency responses.

**Testing requirements:**

- Run the existing test suite (typically via `pytest` or `make test`) to confirm all passing tests remain passing.
- Add a test in the appropriate test file verifying that [`group_coordinator.py`](https://github.com/aio-libs/aiokafka/blob/master/aiokafka/coordinator/group_coordinator.py) uses a different connection object than the fetcher when both target the same broker node. You may mock the connection layer and assert on the `(node_id, group)` keys used.
- Write a regression test for the scenario in [issue #137](https://github.com/aio-libs/aiokafka/issues/137): confirm that a blocking fetch request does not delay an offset commit to the coordinator node.

Implement these changes ensuring code style consistency with the existing codebase — Python 3.6+ compatible asyncio style, and type hints where existing code already uses them.

---

*I declare that all written content in this assessment is my own work, created without the use of AI language models or automated writing tools. All technical analysis and documentation reflects my personal understanding and has been written in my own words.*
