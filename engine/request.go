package engine

import (
	"fmt"
	"strings"
)

// Request is the normalised view of an incoming call the rules match on.
type Request struct {
	ClientID, APIKey, IP, Path, Method, Tenant, Tier string
	Headers                                          map[string]string
	Cost                                             int64 // 0 means 1; heavy endpoints can cost more
}

type dimFn func(*Request) string

func resolveDim(name string) (dimFn, error) {
	switch name {
	case "client_id":
		return func(r *Request) string { return r.ClientID }, nil
	case "api_key":
		return func(r *Request) string { return r.APIKey }, nil
	case "ip":
		return func(r *Request) string { return r.IP }, nil
	case "path":
		return func(r *Request) string { return r.Path }, nil
	case "method":
		return func(r *Request) string { return r.Method }, nil
	case "tenant":
		return func(r *Request) string { return r.Tenant }, nil
	case "tier":
		return func(r *Request) string { return r.Tier }, nil
	}
	if h, ok := strings.CutPrefix(name, "header:"); ok && h != "" {
		return func(r *Request) string { return r.Headers[h] }, nil
	}
	return nil, fmt.Errorf("unknown dimension %q", name)
}

func compileMatch(m Match) (func(*Request) bool, error) {
	type attrCheck struct {
		fn   dimFn
		want string
	}
	var attrs []attrCheck
	for k, v := range m.Attrs {
		fn, err := resolveDim(k)
		if err != nil {
			return nil, fmt.Errorf("match.attrs: %w", err)
		}
		attrs = append(attrs, attrCheck{fn, v})
	}
	methods := make(map[string]struct{}, len(m.Methods))
	for _, x := range m.Methods {
		methods[strings.ToUpper(x)] = struct{}{}
	}
	prefix := m.PathPrefix
	return func(r *Request) bool {
		if prefix != "" && !strings.HasPrefix(r.Path, prefix) {
			return false
		}
		if len(methods) > 0 {
			if _, ok := methods[r.Method]; !ok {
				return false
			}
		}
		for _, a := range attrs {
			if a.fn(r) != a.want {
				return false
			}
		}
		return true
	}, nil
}
