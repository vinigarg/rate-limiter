// Package distributed implements the shared-counter tier: atomic Lua scripts
// executed on Redis (central mode) and token leasing (hybrid mode), plus a
// small dependency-free RESP client. In production you may swap the client for
// go-redis by implementing the one-method Client interface (see README).
package distributed

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// Script is a Lua script identified by its SHA1.
type Script struct{ Src, SHA string }

// NewScript computes the SHA1 used for EVALSHA.
func NewScript(src string) *Script {
	h := sha1.Sum([]byte(src))
	return &Script{Src: src, SHA: hex.EncodeToString(h[:])}
}

// Client is the only thing this package needs from Redis: run a script
// atomically and get back a list of integers.
type Client interface {
	Eval(ctx context.Context, s *Script, keys []string, args ...any) ([]int64, error)
}

// RedisError is an error reply from the server.
type RedisError string

func (e RedisError) Error() string { return "redis: " + string(e) }

type conn struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

// RESPClient is a minimal pooled Redis client (RESP2, EVALSHA with EVAL
// fallback). Point it at a primary (or a cluster proxy / single shard);
// shard selection across several Redis nodes belongs to ShardedClient.
type RESPClient struct {
	addr, password string
	dialTimeout    time.Duration
	ioTimeout      time.Duration
	sem            chan struct{}
	idle           chan *conn
}

// RESPOption customises a RESPClient.
type RESPOption func(*RESPClient)

func WithPassword(p string) RESPOption { return func(c *RESPClient) { c.password = p } }
func WithTimeouts(dial, io time.Duration) RESPOption {
	return func(c *RESPClient) { c.dialTimeout, c.ioTimeout = dial, io }
}
func WithPoolSize(n int) RESPOption {
	return func(c *RESPClient) { c.sem, c.idle = make(chan struct{}, n), make(chan *conn, n) }
}

// NewRESPClient connects lazily.
func NewRESPClient(addr string, opts ...RESPOption) *RESPClient {
	c := &RESPClient{addr: addr, dialTimeout: 200 * time.Millisecond, ioTimeout: 100 * time.Millisecond}
	WithPoolSize(64)(c)
	for _, o := range opts {
		o(c)
	}
	return c
}

func (c *RESPClient) get(ctx context.Context) (*conn, error) {
	select {
	case c.sem <- struct{}{}:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	select {
	case cn := <-c.idle:
		return cn, nil
	default:
	}
	d := net.Dialer{Timeout: c.dialTimeout}
	nc, err := d.DialContext(ctx, "tcp", c.addr)
	if err != nil {
		<-c.sem
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	cn := &conn{c: nc, r: bufio.NewReaderSize(nc, 4096), w: bufio.NewWriterSize(nc, 4096)}
	if c.password != "" {
		if _, err := c.roundTrip(ctx, cn, "AUTH", c.password); err != nil {
			nc.Close()
			<-c.sem
			return nil, err
		}
	}
	return cn, nil
}

func (c *RESPClient) put(cn *conn, healthy bool) {
	if healthy {
		select {
		case c.idle <- cn:
			<-c.sem
			return
		default:
		}
	}
	cn.c.Close()
	<-c.sem
}

// Eval runs the script (EVALSHA, falling back to EVAL on NOSCRIPT).
func (c *RESPClient) Eval(ctx context.Context, s *Script, keys []string, args ...any) ([]int64, error) {
	cn, err := c.get(ctx)
	if err != nil {
		return nil, err
	}
	cmd := make([]any, 0, 3+len(keys)+len(args))
	cmd = append(cmd, "EVALSHA", s.SHA, len(keys))
	for _, k := range keys {
		cmd = append(cmd, k)
	}
	cmd = append(cmd, args...)
	reply, err := c.roundTrip(ctx, cn, cmd[0].(string), cmd[1:]...)
	var re RedisError
	if errors.As(err, &re) && strings.HasPrefix(string(re), "NOSCRIPT") {
		cmd[0], cmd[1] = "EVAL", s.Src
		reply, err = c.roundTrip(ctx, cn, "EVAL", cmd[1:]...)
	}
	if err != nil {
		c.put(cn, errors.As(err, &re)) // server-level errors keep the conn healthy
		return nil, err
	}
	c.put(cn, true)
	arr, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("redis: unexpected script reply %T", reply)
	}
	out := make([]int64, len(arr))
	for i, v := range arr {
		n, ok := v.(int64)
		if !ok {
			return nil, fmt.Errorf("redis: non-integer element %T", v)
		}
		out[i] = n
	}
	return out, nil
}

func (c *RESPClient) roundTrip(ctx context.Context, cn *conn, name string, args ...any) (any, error) {
	dl := time.Now().Add(c.ioTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(dl) {
		dl = d
	}
	_ = cn.c.SetDeadline(dl)
	cn.w.WriteString("*" + strconv.Itoa(1+len(args)) + "\r\n")
	writeBulk(cn.w, name)
	for _, a := range args {
		switch v := a.(type) {
		case string:
			writeBulk(cn.w, v)
		case int:
			writeBulk(cn.w, strconv.Itoa(v))
		case int64:
			writeBulk(cn.w, strconv.FormatInt(v, 10))
		case float64:
			writeBulk(cn.w, strconv.FormatFloat(v, 'g', -1, 64))
		default:
			writeBulk(cn.w, fmt.Sprint(v))
		}
	}
	if err := cn.w.Flush(); err != nil {
		return nil, err
	}
	return readReply(cn.r)
}

func writeBulk(w *bufio.Writer, s string) {
	w.WriteString("$" + strconv.Itoa(len(s)) + "\r\n")
	w.WriteString(s)
	w.WriteString("\r\n")
}

func readReply(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 {
		return nil, errors.New("redis: short reply")
	}
	body := line[1 : len(line)-2]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return nil, RedisError(body)
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n < 0 {
			return nil, err
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n < 0 {
			return nil, err
		}
		out := make([]any, n)
		for i := range out {
			if out[i], err = readReply(r); err != nil {
				return nil, err
			}
		}
		return out, nil
	}
	return nil, fmt.Errorf("redis: bad reply type %q", line[0])
}
