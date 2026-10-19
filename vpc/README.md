# Two-host benchmark (milestone 2)

Loopback (`zig build bench`) is the floor. This measures the real thing: a
client on one host, `vpc_server` on another. It does not care what kind of
hosts they are, so the same steps give a cloud number (same zone, cross-zone)
or a real-network check between two machines on a wired LAN. Only the cloud
runs in the placement the consumer will use are the milestone-2 number.

## Setup

1. Pick the pair and the placement you want to know about, and use the machine
   type the consumer will run on: the pool ceiling and the tail both depend on
   core count.
2. Build here for the hosts' architecture and copy the binaries over:

   ```
   zig build vpc -Dtarget=aarch64-linux-gnu     # or x86_64-linux-gnu
   scp zig-out/bin/vpc_server zig-out/bin/vpc_bench vpc/run.sh <hosts>
   ```

3. Make port 50051 (the default) reachable from the client to the server, in
   whatever your network calls that: firewall rule, security group. Also allow
   ICMP, or `run.sh` has no network floor to report. Addresses must be IPs,
   not hostnames; use private ones.
4. Server: `./vpc_server --listen 0.0.0.0:50051`
5. Client: `./run.sh <server-ip>:50051 <label>` (it uses a `vpc_bench` next to
   it, else `zig-out/bin/`; the ICMP floor needs root or passwordless `sudo`),
   where the label names the
   placement, e.g. `same-zone-<machine-type>` or `lan-wired`.

`run.sh` writes `vpc-<label>-<timestamp>.txt` in the current directory. Per
repetition it runs ICMP RTT, single-caller latency at three payload sizes,
fixed-rate open-loop runs, and saturation. At the defaults (3 repetitions, 30s
per measurement plus 5s warmup) that is about 15 minutes; `REPS` and `DURATION`
scale it.

## Reading it

- Compare p50 against the RTT line. The difference is what zgrpc and the two
  kernels add; the loopback figure (~27us on a Raspberry Pi 5, see
  docs/notes.md) is the expected size of it.
- Take p99/p999 from the `--rate` rows. Closed-loop rows under-sample stalls
  (see the header of `examples/vpc_bench.zig`).
- Open-loop rows at low rates include timer-wake latency on an idle core. That
  is real for a bursty caller, but it is not the transport.
- A bad tail with a clean RTT line is the library, per the notes in
  CLAUDE.md. A bad tail that tracks the RTT line's max is the network or a
  noisy neighbour on a shared host; rerun on another pair before concluding
  anything.
- Run each placement at least three times, on hosts that have been up a few
  minutes. Record the numbers in docs/notes.md with the machine types,
  placement and date.
