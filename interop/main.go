// Interop test: drives the Zig zgrpc server with the reference grpc-go client.
//
// The point is to prove zgrpc speaks real gRPC rather than a dialect its own
// client happens to agree with. grpc-go enforces the parts that are easy to get
// subtly wrong — the HTTP/2 preface and SETTINGS exchange, `te: trailers`,
// content-type negotiation, the 5-byte message prefix, and above all that
// `grpc-status` arrives in a *trailing* HEADERS frame after the body.
//
// A raw byte codec is used so no .proto or generated stubs are needed; the
// payloads are opaque bytes echoed back by the server.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// rawCodec passes bytes through untouched. It reports the name "proto" so the
// content-type stays application/grpc+proto, exactly as a generated stub would.
type rawCodec struct{}

func (rawCodec) Name() string { return "proto" }

func (rawCodec) Marshal(v any) (mem.BufferSlice, error) {
	b, ok := v.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("rawCodec: expected *[]byte, got %T", v)
	}
	return mem.BufferSlice{mem.SliceBuffer(*b)}, nil
}

func (rawCodec) Unmarshal(data mem.BufferSlice, v any) error {
	b, ok := v.(*[]byte)
	if !ok {
		return fmt.Errorf("rawCodec: expected *[]byte, got %T", v)
	}
	*b = data.Materialize()
	return nil
}

type result struct {
	name string
	err  error
}

var results []result

func check(name string, err error) {
	results = append(results, result{name, err})
	if err != nil {
		fmt.Printf("FAIL  %s: %v\n", name, err)
	} else {
		fmt.Printf("ok    %s\n", name)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: interop <address>")
		os.Exit(2)
	}
	addr := os.Args[1]

	cc, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodecV2(rawCodec{})),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial: %v\n", err)
		os.Exit(1)
	}
	defer cc.Close()

	call := func(ctx context.Context, method string, req []byte) ([]byte, error) {
		var resp []byte
		err := cc.Invoke(ctx, method, &req, &resp)
		return resp, err
	}

	// 1. A plain unary echo. Exercises the whole happy path including the
	//    trailing HEADERS frame carrying grpc-status: 0.
	check("unary echo round-trips", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := call(ctx, "/zgrpc.test.Echo/Call", []byte("hello from grpc-go"))
		if err != nil {
			return err
		}
		if string(resp) != "hello from grpc-go" {
			return fmt.Errorf("got %q", resp)
		}
		return nil
	}())

	// 2. An empty message is legal and must not be confused with "no message".
	check("empty message round-trips", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := call(ctx, "/zgrpc.test.Echo/Call", []byte{})
		if err != nil {
			return err
		}
		if len(resp) != 0 {
			return fmt.Errorf("expected empty, got %d bytes", len(resp))
		}
		return nil
	}())

	// 3. A payload spanning multiple DATA frames, which also forces
	//    flow-control accounting to be right.
	check("large message spans frames", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		big := make([]byte, 128*1024)
		for i := range big {
			big[i] = byte(i % 251)
		}
		resp, err := call(ctx, "/zgrpc.test.Echo/Call", big)
		if err != nil {
			return err
		}
		if len(resp) != len(big) {
			return fmt.Errorf("length %d != %d", len(resp), len(big))
		}
		for i := range big {
			if resp[i] != big[i] {
				return fmt.Errorf("byte %d differs", i)
			}
		}
		return nil
	}())

	// 4. A non-OK status must surface as a gRPC status with the right code and
	//    message, not as a transport error.
	check("error status decodes with code and message", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := call(ctx, "/zgrpc.test.Echo/Fail", []byte("x"))
		st, ok := status.FromError(err)
		if !ok {
			return fmt.Errorf("not a grpc status: %v", err)
		}
		if st.Code() != codes.PermissionDenied {
			return fmt.Errorf("code = %v, want PermissionDenied", st.Code())
		}
		if st.Message() != "denied by policy" {
			return fmt.Errorf("message = %q", st.Message())
		}
		return nil
	}())

	// 5. An unknown method must be UNIMPLEMENTED.
	check("unknown method is UNIMPLEMENTED", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := call(ctx, "/zgrpc.test.Echo/Missing", []byte("x"))
		st, _ := status.FromError(err)
		if st.Code() != codes.Unimplemented {
			return fmt.Errorf("code = %v, want Unimplemented", st.Code())
		}
		return nil
	}())

	// 6. Sequential calls must reuse the one connection, each on a new stream.
	check("sequential calls reuse the connection", func() error {
		for i := 0; i < 10; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			payload := []byte(fmt.Sprintf("call-%d", i))
			resp, err := call(ctx, "/zgrpc.test.Echo/Call", payload)
			cancel()
			if err != nil {
				return fmt.Errorf("call %d: %w", i, err)
			}
			if string(resp) != string(payload) {
				return fmt.Errorf("call %d: got %q", i, resp)
			}
		}
		return nil
	}())

	// Server streaming. grpc-go enforces the parts a self-consistent
	// implementation can get away with ignoring: that the response is a single
	// HEADERS frame followed by many messages, that io.EOF arrives only after
	// the trailing HEADERS, and that a mid-stream failure is reported in
	// trailers rather than by dropping the stream.
	streamDesc := &grpc.StreamDesc{StreamName: "Count", ServerStreams: true}

	recvAll := func(ctx context.Context, method string, req []byte) ([][]byte, error) {
		st, err := cc.NewStream(ctx, streamDesc, method)
		if err != nil {
			return nil, err
		}
		if err := st.SendMsg(&req); err != nil {
			return nil, err
		}
		if err := st.CloseSend(); err != nil {
			return nil, err
		}
		var out [][]byte
		for {
			var msg []byte
			err := st.RecvMsg(&msg)
			if errors.Is(err, io.EOF) {
				return out, nil
			}
			if err != nil {
				return out, err
			}
			out = append(out, msg)
		}
	}

	check("server stream delivers every message in order", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		msgs, err := recvAll(ctx, "/zgrpc.test.Echo/Count", []byte("5"))
		if err != nil {
			return err
		}
		if len(msgs) != 5 {
			return fmt.Errorf("got %d messages, want 5", len(msgs))
		}
		for i, m := range msgs {
			want := fmt.Sprintf("item-%d", i)
			if string(m) != want {
				return fmt.Errorf("message %d = %q, want %q", i, m, want)
			}
		}
		return nil
	}())

	check("empty server stream ends cleanly", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		msgs, err := recvAll(ctx, "/zgrpc.test.Echo/Count", []byte("0"))
		if err != nil {
			return err
		}
		if len(msgs) != 0 {
			return fmt.Errorf("got %d messages, want 0", len(msgs))
		}
		return nil
	}())

	check("long server stream survives many messages", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		msgs, err := recvAll(ctx, "/zgrpc.test.Echo/Count", []byte("1000"))
		if err != nil {
			return err
		}
		if len(msgs) != 1000 {
			return fmt.Errorf("got %d messages, want 1000", len(msgs))
		}
		return nil
	}())

	check("mid-stream failure reports in trailers after the messages", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		msgs, err := recvAll(ctx, "/zgrpc.test.Echo/CountThenFail", []byte(""))
		if len(msgs) != 2 {
			return fmt.Errorf("got %d messages before failure, want 2", len(msgs))
		}
		st, ok := status.FromError(err)
		if !ok {
			return fmt.Errorf("not a grpc status: %v", err)
		}
		if st.Code() != codes.DataLoss {
			return fmt.Errorf("code = %v, want DataLoss", st.Code())
		}
		if st.Message() != "ran out of data" {
			return fmt.Errorf("message = %q", st.Message())
		}
		return nil
	}())

	// 11. Concurrent RPCs multiplexed over the one connection grpc-go keeps
	//     open. Each handler sleeps 300ms, so eight of them served in
	//     sequence would take 2.4s and served at once take about 300ms. The
	//     bound is set well between the two: this is a check that the streams
	//     genuinely overlap, not a benchmark.
	check("concurrent streams multiplex over one connection", func() error {
		const concurrency = 8
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		errs := make(chan error, concurrency)
		started := time.Now()
		for i := 0; i < concurrency; i++ {
			go func(i int) {
				payload := []byte(fmt.Sprintf("call-%d", i))
				resp, err := call(ctx, "/zgrpc.test.Echo/Slow", payload)
				if err != nil {
					errs <- err
					return
				}
				// Also proves responses are not crossed between streams.
				if string(resp) != string(payload) {
					errs <- fmt.Errorf("got %q, want %q", resp, payload)
					return
				}
				errs <- nil
			}(i)
		}
		for i := 0; i < concurrency; i++ {
			if err := <-errs; err != nil {
				return err
			}
		}

		if elapsed := time.Since(started); elapsed > 1500*time.Millisecond {
			return fmt.Errorf("%d concurrent calls took %v; served sequentially", concurrency, elapsed)
		}
		return nil
	}())

	// 12. Custom metadata. grpc-go lowercases keys and sends them as ordinary
	//    HPACK-compressed headers, with its own choice of indexing; what is
	//    checked is that the handler sees them under the names a stub's caller
	//    used.
	check("request metadata from grpc-go reaches the handler", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "X-Request-Id", "req-7", "authorization", "Bearer abc")
		resp, err := call(ctx, "/zgrpc.test.Echo/Metadata", nil)
		if err != nil {
			return err
		}
		if want := "id=req-7;auth=Bearer abc"; string(resp) != want {
			return fmt.Errorf("handler saw %q, want %q", resp, want)
		}
		return nil
	}())

	// 13. grpc-go picks its own units for `grpc-timeout` — it sends whatever
	//     of "n", "u", "m", "S"… fits the remaining time — so decoding what
	//     *it* sends is a different check from decoding what we send.
	check("grpc-timeout from grpc-go arrives as a usable deadline", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		resp, err := call(ctx, "/zgrpc.test.Echo/DeadlineReport", nil)
		if err != nil {
			return err
		}
		if string(resp) == "none" {
			return errors.New("server saw no deadline")
		}
		left, convErr := strconv.Atoi(string(resp))
		if convErr != nil {
			return fmt.Errorf("unparseable reply %q", resp)
		}
		if left <= 0 || left > 2000 {
			return fmt.Errorf("server reported %dms left of a 2000ms deadline", left)
		}
		return nil
	}())

	// 14. A call that outruns its deadline. The handler sleeps 300ms against a
	//     100ms deadline, so the status must be DeadlineExceeded and it must
	//     arrive without waiting out the handler.
	check("a call past its deadline fails with DEADLINE_EXCEEDED", func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()

		started := time.Now()
		_, err := call(ctx, "/zgrpc.test.Echo/Slow", []byte("too slow"))
		elapsed := time.Since(started)

		st, ok := status.FromError(err)
		if !ok {
			return fmt.Errorf("not a grpc status: %v", err)
		}
		if st.Code() != codes.DeadlineExceeded {
			return fmt.Errorf("code = %v, want DeadlineExceeded", st.Code())
		}
		if elapsed > 250*time.Millisecond {
			return fmt.Errorf("took %v; waited for the handler rather than the deadline", elapsed)
		}
		return nil
	}())

	failed := 0
	for _, r := range results {
		if r.err != nil {
			failed++
		}
	}
	fmt.Printf("\n%d/%d interop checks passed\n", len(results)-failed, len(results))
	if failed > 0 {
		os.Exit(1)
	}
}
