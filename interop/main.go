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
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/mem"
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
