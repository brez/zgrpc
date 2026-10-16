# In-VPC benchmark (milestone 2)

Loopback (`zig build bench`) is the floor. This measures the real thing: a
client on one instance, `vpc_server` on another.

## Setup

1. Two instances in the placement you want to know about. Run same-AZ first,
   then cross-AZ. Use the instance type the consumer will run on; the pool
   ceiling and the tail both depend on core count.
2. Build here for the instance's architecture and copy the binaries over:

   ```
   zig build vpc -Dtarget=aarch64-linux-gnu     # or x86_64-linux-gnu
   scp zig-out/bin/vpc_server zig-out/bin/vpc_bench vpc/run.sh <hosts>
   ```

3. Allow the port (default 50051) from the client to the server in the
   security group. Addresses are IPs, not hostnames; use private ones.
4. Server: `./vpc_server --listen 0.0.0.0:50051`
5. Client: `./run.sh <server-private-ip>:50051 same-az-<instance-type>`

`run.sh` does three repetitions of: ICMP RTT, single-caller latency at three
payload sizes, fixed-rate open-loop runs, and saturation. About 25 minutes.

## Reading it

- Compare p50 against the RTT line. The difference is what zgrpc and the two
  kernels add; the loopback figure (~27us) is the expected size of it.
- Take p99/p999 from the `--rate` rows. Closed-loop rows under-sample stalls
  (see the header of `examples/vpc_bench.zig`).
- Open-loop rows at low rates include timer-wake latency on an idle core. That
  is real for a bursty caller, but it is not the transport.
- A bad tail with a clean RTT line is the library, per the notes in
  CLAUDE.md. A bad tail that tracks the RTT line's max is the network or the
  noisy-neighbour host; rerun on another pair before concluding anything.
- Run each placement at least three times, on a pair of instances that have
  been up a few minutes. Record the numbers in docs/notes.md with the instance
  types, AZs and date.
