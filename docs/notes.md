# Design notes

Why zgrpc is built the way it is, and what measuring it turned up. None of this
is needed to use the library — see the [README](../README.md) for that. It is
here because the reasoning is worth more than the conclusions: most of it is
the record of something that turned out not to be true.

## Latency

`zig build bench` measures unary calls over loopback, so it reports the
library's own cost with network RTT removed. It is a floor, not a prediction:
the milestone-2 number has to come from the real VPC, where same-AZ vs
cross-AZ RTT dominates the ~5ms budget.

Reference numbers (Raspberry Pi 5, loopback, ReleaseFast, idle machine), taken
on the single-task-per-connection transport that preceded the concurrent one:

```
     64 B  n=10000   mean    18.6us  p50    18.5us  p99   21.1us  p999    32.5us
   1024 B  n=10000   mean    19.8us  p50    19.5us  p99   29.3us  p999    33.1us
  16384 B  n=10000   mean    41.7us  p50    44.6us  p99   71.9us  p999   273.4us
 131072 B  n= 2000   mean   342.8us  p50   318.1us  p99  658.1us  p999  1379.5us
```

Current numbers, same machine, concurrent transport, quiet (browsers closed,
load average under 0.5 at start; three consecutive runs, range shown):

```
     64 B  p50  25.6-27.5us  p99  30.4-37.3us  p999   74.5-215.0us
   1024 B  p50  27.3-28.2us  p99  40.8-71.9us  p999  591.2-985.6us
  16384 B  p50  46.7-49.8us  p99  90.3-128.6us p999 485.5-1554.5us
 131072 B  p50 396.7-418.6us p99 1096-1576us   p999 3888-5270us
```

p50 is stable to a few percent; p99 and especially p999 are not, and one run's
tail should not be quoted. The governor was still `ondemand`, not `performance`.
These p50s run 3-4us under the "after" column in the next section (25.6us vs
29.0us at 64 B, 27.3us vs 31.0us at 1 KB), so the handoff cost there, measured
loaded, was overstated: about 8us over the old transport, not 11.

### What concurrency cost

The dedicated reader task (see "Concurrency") puts a thread handoff in the
middle of every call: the reply is parsed by the connection's reader and the
caller is woken to collect it. That is not free, and it was measured rather
than assumed — old transport and new, built from the same tree and run
back to back:

```
                 p50 before   p50 after
     64 B            17.8us      29.0us
   1024 B            18.2us      31.0us
  16384 B            45.0us      55.0us
 131072 B           360.0us     530.0us
```

About 11µs of that is one context switch, and it is the price of the whole
feature: concurrent calls and cancellation that does not wait for a window to
close. Against a ~5ms in-VPC budget it is 0.2%. The 128KB gap is larger
because a multi-frame message crosses the handoff several times.

Two things were found chasing it, both worth more than the handoff itself:

* **Wakeups were being spent three-to-a-call.** A unary response is HEADERS,
  DATA and trailers, written with one flush and therefore read in one go — but
  the reader woke the caller after each frame. Deferring wakeups until the
  reader is actually about to block on the socket collapses them into one, and
  took the 64-byte p99 from ~610µs to ~230µs.
* **The per-stream receive buffer compacted on every read**, making a message
  that spans many frames quadratic in its own size. Consumed space is now
  reclaimed by the append that needs the room.

These numbers are an A/B, not a result: the machine had a browser on it and
its load shows in every tail figure — the *old* code measured p999 609µs to
2.1ms on the same runs, against the 32.5µs in the table above. The table stands
as the last clean measurement; a clean re-measurement on an idle machine is
owed before any of it is quoted.

Getting there found a real bug worth recording, because it is exactly the kind
of tail problem the project brief warns about — and it was *not* the HTTP/2
library:

```
before TCP_NODELAY:  64 B  p50    95.5us  p99 2500.3us  p999 48026.8us
after  TCP_NODELAY:  64 B  p50    18.5us  p99   21.1us  p999    32.5us
```

A p50 of 95µs beside a p999 of 48ms — a 1,478x tail — is the signature of
Nagle's algorithm meeting the peer's delayed ACK. gRPC is a ping-pong of small
writes, which is precisely the pattern where the sender withholds a small
segment awaiting an ACK the receiver is withholding awaiting more data; on
Linux the standoff breaks only when the 40ms delayed-ACK timer fires.
`std.Io.net` does not set TCP_NODELAY, so `src/tcp.zig` sets it on both
accepted and dialled sockets.

Two lessons for milestone 2: percentiles were essential (the mean and p50 both
looked fine while one call in a thousand took 48ms), and a bad tail is worth
attributing before optimising — the brief's guess that a bad tail would
implicate the HTTP/2 library would have sent the investigation the wrong way
here.

The benchmark also deliberately avoids `DebugAllocator`, which captures a stack
trace per allocation and on ARM costs more than the RPC being measured.

### Flow-control windows

We advertise a 256KB stream window and raise the connection window to 1MB,
rather than living with HTTP/2's 65,535-byte default. The default is a latency
bug at any real bandwidth: a sender may have only that much in flight before it
must stop and wait for credit, so once bandwidth times round-trip exceeds it,
every message larger than the window costs an extra round trip. In-VPC at
10Gbps the bandwidth-delay product is around half a megabyte — the default
misses it by an order of magnitude.

The connection window cannot be set in SETTINGS; RFC 9113 6.9.2 fixes its
initial value and only a WINDOW_UPDATE on stream 0 moves it, so that frame goes
out alongside our SETTINGS.

The cost is memory, and it is deliberate: the connection window is exactly the
bound on what a peer can make one connection buffer before we have consumed any
of it. Multiplied by `Server.Options.max_concurrent_connections`, it is the
server's worst case. Credit is returned when a caller *consumes* bytes, not
when they arrive — returning it on arrival, as an earlier version did, means
there is no bound at all.

## In the VPC (milestone 2)

2026-10-19, AWS us-east-1, three c7g.large (2 vCPU, arm64, Amazon Linux 2023):
one server in 1a, a client in 1a, a client in 1c. Built `ReleaseSafe`. Three
repetitions of `vpc/run.sh`, 30s per row, no failed calls. Ranges across the
repetitions; p99/p999 are from the fixed-rate rows, which do not under-sample
stalls (the one-caller rows stop recording at 200,000 samples).

```
                               same-AZ (1a-1a)        cross-AZ (1c-1a)
ICMP RTT avg / max             0.125 / 1.76 ms        0.56 / 2.84 ms
1 caller, 1 KB   p50           58-61us                454-520us
1 caller, 16 KB  p50           92-100us               519-539us
500-5000/s, 8 callers  p50     222-238us              594-633us
                       p99     275-295us              670-721us
                       p999    311-509us              738-881us
8 callers, flat out            ~53k/s, p50 107-113us  ~16.5k/s, p50 477-491us
64 callers, flat out           93-98k/s, p999 7-9ms   88-89k/s, p999 6-8ms
```

Against a ~5ms budget the worst fixed-rate p999 is under 1ms, so there is no
sign of a library tail problem. The 64-caller tail is two vCPUs saturated at
~95k calls/s, not a load anyone will offer. Cross-AZ costs about 400us, nearly
all network.

Two cautions. The same-AZ RPC p50 sits below the ICMP average: a ping every
50ms lets the core idle, so ICMP overstates the floor there and "p50 minus RTT"
is not the RPC's cost. And this is one host pair at one time of day; noisy
neighbours are not ruled out. Only 1a-1c was run, because c7g.large is not
offered in 1b, and the TSDB's own AZ layout is not recorded.

## Concurrency

Three levels, each on its own unit of concurrency:

* **A connection per accept.** `serve` spawns each accepted connection and
  keeps accepting. Bounded by `Options.max_concurrent_connections`.
* **A reader per connection.** One task owns the read side and does nothing
  else: it pulls frames off the socket, decodes header blocks, and files
  everything into per-stream mailboxes. Callers never read from the socket.
* **A call per stream.** Each stream the peer opens is handled on its own task,
  so a handler that is slow, blocked, or streaming a million rows holds up
  nothing but itself. Clients get the same thing from the other side: `call`
  may be invoked from several tasks on one `Client`, and the calls multiplex.

Writers take a separate lock from connection state and the two are never held
at once, so a writer blocked in `flush` cannot stall frame processing — which
is what would deadlock against a peer that is itself blocked writing to us.

A dedicated reader is what makes cancellation prompt. When the writer drove the
reader, a RST_STREAM sat unread until the writer next happened to block for
flow-control credit, so a server noticed a cancelled call only as promptly as
the peer's window was small — and grpc-go grows windows dynamically. Now the
frame is seen on arrival and the next `stream.send` fails, whatever the window
is doing.

The one rule the structure exists to keep is that a connection's header blocks
are decoded on exactly one task. `Hpack.decodeHeaderFieldView` in the
dependency calls `ScratchBuffer.reset()` on entry and returns slices into that
buffer, which is `threadlocal`; its dynamic table is connection-wide ordered
state. So different threads decoding concurrently are fine, and two of one
connection's blocks interleaved on a single thread are not. Decoding lives in
the reader task alone, which settles it — and `src/h2.zig` still copies each
header into an arena before decoding the next, because a lazy copy would
corrupt all but the last.

## Connection pooling

`Client` is one connection. `Pool` is several to the same peer, and is what
you want unless you have a reason not to — it is also where connection
lifecycle lives, since a `Client` whose peer has gone away stays broken.

HTTP/2 multiplexes, so the question is why more than one connection is ever
worth having. Two reasons, both of which show up only under concurrency: the
peer's `SETTINGS_MAX_CONCURRENT_STREAMS` caps calls in flight on a connection,
and one connection's frames are parsed by one reader task, so past some call
rate that task saturates a core.

The policy is: take an idle connection if there is one; failing that, open
another up to `Options.size`; failing that, share the least loaded. Growing
before sharing matters because the only reason to want a second connection is
that the first is busy — and it means the ceiling is free until concurrency
justifies it. A caller issuing one call at a time holds exactly one connection
whatever the ceiling is.

Measured on loopback (`zig build bench`, "under load", 1KB payload):

```
   callers 8    pool 1   p50   116-119us  p99   298-380us   p999 1.6-2.0ms   58-61k calls/s
   callers 8    pool 4   p50    86-89us   p99   193-424us   p999 1.1-3.2ms   71-81k calls/s
   callers 64   pool 1   p50  1274-1289us p99  5.5-5.8ms    p999 8.5-10.6ms  49-51k calls/s
   callers 64   pool 4   p50   735-757us  p99  3.3-3.6ms    p999 5.3-6.0ms   69-71k calls/s
```

Pool 4 over pool 1: +25-37% throughput and 27-42% off p50 at both concurrency
levels (the earlier loaded figures said ~30% for both), which is
where the default ceiling of 4 comes from. It is a property of the machine —
one reader task per connection, so the useful ceiling tracks cores — and worth
re-measuring on the target host. Measured on a quiet machine (three runs).

A connection the peer has closed or sent GOAWAY on is retired at the next call
and replaced. Retirement is not cancellation: calls already running on it are
left to finish, and it is closed when the last one does. Nothing is retried —
the call in flight when a connection dies fails, and it is the caller's
business whether to try again.

## Deadlines

`CallOptions.deadline_ns` is sent to the server as `grpc-timeout` and enforced
locally. Local enforcement is the half that matters: a server that has hung,
or one that ignores the header, must not be able to hang the caller. Every
point where a call waits is bounded by it — finding a stream to run on, waiting
for flow-control credit, waiting for the reply — and the call fails with
`error.DeadlineExceeded`.

That is deliberately an error rather than a `.deadline_exceeded` status. The
two are different facts: the error is *this* client's clock running out, and
the status is the server reporting that it gave up. A caller that wants to
treat them alike can, but it should have to say so.

Server-side, `grpc-timeout` becomes `ctx.deadline`, and the transport enforces
it wherever it blocks on the client's behalf — reading the request, and
`ServerStream.send`. The clock starts when the request headers are read, not
when the client sent them: `grpc-timeout` is a duration, and the time it spent
in flight is not knowable without synchronised clocks. The server's deadline is
therefore always slightly later than the client's, which is the safe direction.

```zig
fn rangeQuery(ctx: *zgrpc.CallContext, request: []const u8, stream: *zgrpc.ServerStream) anyerror!void {
    // Bound your own work with the same limit the call has.
    const rows = try openCursor(ctx.allocator, request, ctx.deadlineTimeout());
    while (try rows.next()) |row| {
        // Fails with error.DeadlineExceeded once the deadline passes, which
        // unwinds the handler.
        try stream.send(row);
    }
}
```

What the server cannot do is interrupt a handler's own work — there is no way
to preempt a function. A handler that spends a minute on CPU will spend a
minute on CPU, and the client will have given up long before. `ctx.expired()`,
`ctx.remaining()` and `ctx.deadlineTimeout()` exist so a handler that cares can
cooperate; nothing forces it to.

Deadlines are measured on a clock unaffected by system time changes, so a call
given five seconds does not become a call given an hour because NTP stepped
backwards.

## Rapid reset

CVE-2023-44487: open a stream and reset it at once. The client's own stream
slot is free the moment it resets, so it can do this as fast as it can write;
the server has to decode the headers and start a call for each. `zig build
flood` builds `reset_flood`, which mounts the attack with the client's own API
(`openStream` then `closeStream`) against `vpc_server`, while a second
connection makes sequential 64-byte calls. `--mode calls` is the control: the
same number of generators making ordinary calls. Server threads and RSS were
sampled from /proc/<pid>. A 4-core Raspberry Pi with everything on it, so
read the shapes, not the absolute figures.

What bounds it already: a stream stays in the table, counted against the 128
concurrent-stream limit, until its call task has finished, however early the
peer reset it. Resetting does not buy the attacker a fresh slot, which is the
whole of the original vulnerability in servers that freed it. So concurrent work
was never unbounded.

What it cost anyway. Before: every accepted stream got a task, which under
`std.Io.Threaded` is a thread, so a flood ran the server up to its cap in live
threads.

| load on the server                      | legit p99 | legit calls / 5s | server threads | RSS   |
|-----------------------------------------|----------:|-----------------:|---------------:|------:|
| none                                    |   0.3 ms  |         ~100,000 |              - |     - |
| 4 ordinary-call generators (control)    |   0.7 ms  |           48,205 |             23 | 12 MB |
| 1 reset attacker, ~157k resets/s        |   1.55 ms |           33,539 |            139 | 46 MB |
| 4 reset attackers, ~468k resets/s       |   7.3 ms  |            9,711 |             87 | 32 MB |

The control is the point: resets cost something ordinary load does not.

The fix is small. `acceptStream` now drops a stream that was reset while it
waited instead of handing it out, so no task is started to find out on its first
read. After, one attacker, ~190k resets/s: p99 0.70 ms, 68,250 legit calls,
10 threads, 12.6 MB, indistinguishable from the control on resources. Four
attackers, ~511k resets/s: threads 19, RSS 13 MB, but p99 is still 7.2 ms and
legit calls 9,455. That is CPU saturation, five busy reader tasks on four
cores, not a resource leak, and only a limit on the reset rate would address
it.

That limit is `Server.Options.reset_limit`: a token bucket per connection,
default 2,000 resets/s with a burst of 4,000, counting every RST_STREAM
including those for streams already gone. A peer that runs it dry gets
GOAWAY(ENHANCE_YOUR_CALM) naming its last stream, then the connection is
closed. Same four attackers, same machine: all four disconnected within the
burst (about 50,000 streams between them, against 2.5 million before), the
bystander's p99 347 us against 273 us idle, 95,626 calls against 9,455. The
default is far above ordinary traffic; the test for that has a caller abandon a
hundred streaming calls in a row. A pool whose callers all time out at once
during an outage is the case that could trip it, which is why it is an option
and `null` turns it off.

Not done: a call whose HEADERS were accepted and whose RST arrives afterwards
still runs its handler to the first read or write that notices. A stream
handler does that promptly; a unary handler that is purely CPU does not,
because nothing preempts it.

## Interop findings

Bugs found this way that the internal tests missed:

- Stream-level `WINDOW_UPDATE` frames were ignored entirely; only the
  connection window was tracked.
- A sender blocked on a closed window consumed the very `WINDOW_UPDATE` it was
  waiting for and kept blocking. Any message over ~32KB deadlocked.
- The flow-control wait loop discarded every frame it read, so a request opened
  while a long response was still being written was silently lost. Frames that
  do not belong to the writer are now stashed for the next reader.

Adding server streaming surfaced one more, caught by an internal test rather
than interop: skipping a HEADERS frame for a stream we were not reading
desynchronised HPACK. The dynamic table is connection-wide state built by every
header block in order, so a skipped block corrupts every later one. Header
blocks are now always decoded, even when discarded.

Making the transport concurrent surfaced two of its own, both found by
benchmarking rather than by a test — see "What concurrency cost": wakeups spent
three to a call, and a per-stream receive buffer that compacted on every read
and so was quadratic in message size.

## Layout

```
src/h2.zig               HTTP/2 connection driver: reader task, per-stream
                         mailboxes, flow control (frames + HPACK from http2.zig)
src/framing.zig          gRPC 5-byte length-prefixed message framing
src/status.zig           grpc-status codes, percent-encoded grpc-message
src/timeout.zig          grpc-timeout deadline encoding
src/client.zig           unary + server-streaming client, one connection
src/pool.zig             several persistent connections to one peer
src/server.zig           unary + server-streaming server, task per call
src/tcp.zig              TCP_NODELAY
src/integration_test.zig end-to-end tests over loopback
examples/echo_server.zig standalone server (interop target)
examples/bench.zig       loopback latency and pool throughput
examples/reset_flood.zig rapid-reset load tool (zig build flood)
examples/vpc_*.zig      two-host benchmark for milestone 2 (see vpc/)
interop/                 grpc-go interop suite
packaging/               proof that it works as a published dependency
```
