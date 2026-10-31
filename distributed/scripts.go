package distributed

// All scripts read time from Redis (TIME) so node clock skew never matters,
// run atomically (no interleaving), keep ONE key per limit (cluster-safe via
// the {key} hash tag) and set a TTL so memory is bounded by active keys.
// Each returns integers: {allowed, remaining, retry_after_ms}.

// Token bucket. ARGV: rate_per_ms, burst, cost.
var tokenBucketScript = NewScript(`
local t = redis.call('TIME')
local now = t[1]*1000 + math.floor(t[2]/1000)
local rate, burst, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local d = redis.call('HMGET', KEYS[1], 't', 'ts')
local tokens, ts = tonumber(d[1]), tonumber(d[2])
if tokens == nil then tokens = burst; ts = now end
if now > ts then tokens = math.min(burst, tokens + (now-ts)*rate) end
local allowed, retry = 0, 0
if tokens >= cost then tokens = tokens - cost; allowed = 1
else retry = math.ceil((cost-tokens)/rate) end
redis.call('HSET', KEYS[1], 't', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst/rate)*2 + 1000)
return {allowed, math.floor(tokens), retry}
`)

// Sliding window counter. ARGV: limit, window_ms, cost.
var slidingCounterScript = NewScript(`
local t = redis.call('TIME')
local now = t[1]*1000 + math.floor(t[2]/1000)
local limit, win, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local start = now - (now % win)
local d = redis.call('HMGET', KEYS[1], 'c', 'p', 's')
local c, p, s = tonumber(d[1]) or 0, tonumber(d[2]) or 0, tonumber(d[3]) or start
if s ~= start then
  if start - s == win then p = c else p = 0 end
  c = 0; s = start
end
local f = (now - start) / win
local est = p*(1-f) + c
local allowed, retry = 0, 0
if est + cost <= limit then
  c = c + cost; est = est + cost; allowed = 1
else
  local room = limit - c - cost
  local rem = start + win - now
  retry = rem
  if p > 0 and room >= 0 then
    retry = math.ceil((1 - room/p - f) * win)
  elseif c > 0 then
    retry = rem + math.ceil(math.max(1 - (limit-cost)/c, 0) * win)
  end
  if retry < 1 then retry = 1 end
end
redis.call('HSET', KEYS[1], 'c', c, 'p', p, 's', s)
redis.call('PEXPIRE', KEYS[1], win*2)
local rem = math.floor(limit - est); if rem < 0 then rem = 0 end
return {allowed, rem, retry}
`)

// Fixed window. ARGV: limit, window_ms, cost.
var fixedWindowScript = NewScript(`
local t = redis.call('TIME')
local now = t[1]*1000 + math.floor(t[2]/1000)
local limit, win, cost = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local start = now - (now % win)
local d = redis.call('HMGET', KEYS[1], 'c', 's')
local c, s = tonumber(d[1]) or 0, tonumber(d[2]) or start
if s ~= start then c = 0; s = start end
local allowed, retry = 0, 0
if c + cost <= limit then c = c + cost; allowed = 1 else retry = start + win - now end
redis.call('HSET', KEYS[1], 'c', c, 's', s)
redis.call('PEXPIRE', KEYS[1], win*2)
return {allowed, math.max(limit - c, 0), retry}
`)

// Lease: withdraw up to `want` whole tokens from a global token bucket
// (partial grants allowed). ARGV: rate_per_ms, burst, want. Returns {granted, retry_ms}.
var leaseScript = NewScript(`
local t = redis.call('TIME')
local now = t[1]*1000 + math.floor(t[2]/1000)
local rate, burst, want = tonumber(ARGV[1]), tonumber(ARGV[2]), tonumber(ARGV[3])
local d = redis.call('HMGET', KEYS[1], 't', 'ts')
local tokens, ts = tonumber(d[1]), tonumber(d[2])
if tokens == nil then tokens = burst; ts = now end
if now > ts then tokens = math.min(burst, tokens + (now-ts)*rate) end
local grant = math.min(math.floor(tokens), want)
local retry = 0
if grant >= 1 then tokens = tokens - grant else retry = math.ceil((1-tokens)/rate) end
redis.call('HSET', KEYS[1], 't', tostring(tokens), 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst/rate)*2 + 1000)
return {grant, retry}
`)
