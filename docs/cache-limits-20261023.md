# Bounded cache-limit experiment — 2026-10-23

## Contract

Defaults: `STORE_TTL=10m`, `STORE_MAX_ENTRIES=100`, `STORE_MAX_BYTES=0`.
Each zero limit independently means unlimited. Negative count/byte limits fail startup.
Count means registered response-ID aliases, not shared prefix nodes or socket pins.
Count and live-payload pressure share the existing LRU order (last access); TTL runs
from creation/replacement, not reads. Admission protects its incoming alias. Metadata,
pin, compaction, request and working-set limits remain independent and unchanged.

The experiment profile is **24h / 10,000 aliases / unlimited live payload**. No
production deployment, load, restart or VM reboot was performed by this lane.

## Method and bounds

- Keeper base: `502bbfd`, plus the cache-limit implementation accompanying this document.
- Local Linux x86-64, AMD Ryzen AI 9 365; 20 allowed logical CPUs; ZFS-backed `/var/tmp`.
  Go `go1.27.3-X:nodwarf5`; no CPU quota or production-equivalent cgroup was imposed.
- One keeper process plus in-process fake upstream and driver. `GOMEMLIMIT=128MiB`,
  `GOGC=75`. Reported CPU and RSS include the fake upstream and driver, not just keeper.
- 12,000 sequential HTTP turns, then 100 deterministic random-parent continuations.
  Ten-turn conversations; inputs primarily 4 KiB, with 32-KiB and 512-KiB samples,
  plus three 4-MiB inputs. Histories grow and share prefixes. The upstream hashes every
  user input; every reply is checked against the expected complete history digest.
  Upstream rejects any unexpanded `previous_response_id`.
- Monitor every 100 ms: stop on RSS/high-water RSS above **384 MiB**, aggregate cache
  file logical size above **8 GiB**, or 12 minutes elapsed. Neither completed run hit
  a stop threshold. The second run also records kernel RSS high-water and allocated
  file blocks. This is a monitor-based bound, not a hard memory cgroup/quota.
- Hit/expand probes: 100 each at 100, 5,000 and 10,000 aliases, through actual
  spool/parse/cache lookup/history expansion and spool drain, without a new admission.
  The first expanded request at each level was 524,548 bytes; samples span recent
  ten-turn histories. These times exclude network inference and response retention.

## Results (second completed run)

| Measure | Result |
|---|---:|
| Initial turns / random-parent continuations | 12,000 / 100, all passed |
| Wall time, including probes and reopen | 31.04 s |
| CPU user / system | 55.60 s / 3.76 s |
| Kernel RSS high-water | 198.63 MiB |
| Peak aggregate cache file logical size, sampled | 189.98 MiB |
| Cache database at 10,000 aliases | 172,449,792 bytes (164.46 MiB) |
| Live payload at 10,000 aliases | 136,071,824 bytes (129.77 MiB) |
| Database after 12,100 turns | 198,619,136 bytes (189.42 MiB) |
| Quiescent cache regular files | 3: database, lock, spool-owner marker |
| Peak allocated file blocks, sampled | 8,091,648 bytes |
| Full HTTP turn latency p50 / p95 / max | 2.030 / 6.366 / 60.741 ms |
| Open and wipe existing 10,000-alias database | 24.586 ms |
| Aliases after reopen | **0** (not restored) |

The first completed run also passed (33.96 s, sampled RSS peak 177.07 MiB,
42.289-ms reopen). Differences are local run variability, not a production statistic.
Some preliminary attempts aborted because the monitor raced with spool deletion;
those are excluded, the monitor was fixed, and the two completed runs passed.

| Cached aliases | Hit/expand p50 | p95 | max |
|---:|---:|---:|---:|
| 100 | 0.215 ms | 0.425 ms | 0.923 ms |
| 5,000 | 0.259 ms | 0.551 ms | 0.600 ms |
| 10,000 | 0.272 ms | 0.641 ms | 0.825 ms |

Admission 10,001 kept the new alias, evicted the least-recently-used original alias,
and left exactly 10,000 aliases. The subsequent random-parent tests all expanded
complete histories correctly. Expiry is covered with a deterministic clock in the
normal regression suite; this short test is **not a 24-hour soak**.

The synthetic inputs are deliberately compressible. Filesystem compression and sparse
allocation can make allocated-block usage much smaller than logical file size. Use the
logical figures, not the 8-MB allocated figure, for conservative production sizing.
The production traffic-size distribution was not read or inferred from file count.

## Real ENOSPC and slow-write checks

ENOSPC: an unprivileged user/mount namespace supplied a **64-MiB tmpfs for the database
only**. Request spools stayed on real disk. A filler left 64 KiB free, then a 512-KiB
input forced an actual `write ... keeper.db: no space left on device`. The turn still
returned HTTP 200 / `response.completed` / `store:false`; no rejected alias was readable.
Previously accepted history remained correct, and bbolt `Tx.Check` passed. Removing
the filler allowed retention and continuation from the original parent again.
This isolated tmpfs is only a fault fixture, **not** a deployment recommendation.
If the request spool itself cannot be written, that is an explicit turn failure,
not the cache-retention fallback demonstrated here.

Slow disk: `strace` injected **10-ms delay into each `pwrite64`**, with 18 delayed
writes observed. The first SSE event arrived at 3.347 ms; terminal acceptance at
64.082 ms. Storage and a subsequent continuation both succeeded. This verifies
ordered terminal acceptance and early streaming under delayed cache writes, not a
hardware latency profile, sustained saturation test, or bound on concurrent tail latency.

## Production sizing (read-only snapshot)

Allowed read-only `df -h`, `du -sb`, `ls -la` and file count on the known keeper volume:

- Root and Docker/keeper filesystem: **30 GiB total, 21 GiB used, 7.2 GiB available**
  (`df -h` rounded; 75% used).
- Keeper volume logical directory size: **35,501,548 bytes**; database 28,336,128 bytes;
  **10 regular files** including transient spools. File count is not alias count.
- No configuration, containers, images, filesystem quotas or data were changed.

For this observed **synthetic** mix, 10,000 aliases used about 130 MiB live payload
and 164 MiB database space, rising to about 189 MiB after turnover. Plan for the
high-water original plus a compacted copy and up to 1 GiB transient spool headroom.
Do not extrapolate this average to arbitrary production conversations: 10,000
independent 512-KiB inputs alone total **4.88 GiB**, and a second compacted copy would
already exceed the host's 7.2 GiB free space before outputs and other Docker data.
A live database larger than the unchanged 400-MiB compaction trigger may also trigger
expensive compaction before every admission; the trigger is not a quota.

**Recommendation:** the requested `24h/10000/0` is suitable only as a supervised,
bounded experiment, not an unconditional long-running profile on this shared 30-GiB
host. Keep current production unchanged pending a separate coordinator rollout.
For continued operation, prefer a positive live-payload cap (for example 512 MiB;
confirm compaction/headroom behavior under that actual mix), fewer entries/shorter TTL,
or a larger dedicated volume. Do not automatically change the requested experiment
values or independent safety limits based on this recommendation.

Longer TTL improves client-to-keeper delta reuse/reconnect/rollback, not the complete
history transmitted from keeper to the stateless upstream. Metadata and pin pressure
can still shorten retention. Startup deliberately wipes all aliases, so 24h is never
a restart-persistence promise. Histories are plaintext on disk; expiry/eviction/wipe
are logical deletions, not secure erasure of old pages.

## Proposed production observation window (not executed)

After separately authorized coordinator deployment, use exactly `STORE_TTL=24h`,
`STORE_MAX_ENTRIES=10000`, `STORE_MAX_BYTES=0`. Preserve other safety budgets and
all topology/authentication settings. Start with 30 minutes of normal traffic;
this lane does not authorize extra production load, inference calls, or a reboot.
Sample memory/disk/CPU every 5 seconds and stop expanding the experiment if any:

- Cache directory logical usage reaches **1 GiB**, or filesystem free space drops
  below **4 GiB**. Include database copies and spools, not just live payload counters.
- Keeper RSS or its cgroup memory usage reaches **384 MiB**, or any OOM/restart occurs.
- Keeper CPU averaged over 60 seconds exceeds **one logical core** (Docker 100%) or
  **80% of an existing CPU quota**, whichever is lower. Quota must be freshly read
  by the deployment owner; it was not inspected under this lane's read-only scope.
- Any missing/truncated history, failed bounded continuation smoke, persistent cache
  I/O errors, or service-health regression.

These are external experiment stop gates, not replacements for zero-unlimited
configuration semantics. Stop new test load, preserve diagnostics and use the
coordinator's fresh-read/CAS keeper-only rollback if needed; do not stop the gateway
or reboot the VM. A passed 30-minute window is not a 24-hour soak or cold-boot acceptance.
Longer passive observation needs its own owner and resource budget.

## Evidence and reproduction receipts

Local receipts (not production secrets):
`/var/tmp/keeper-cache-limits-20261023/experiment/{pressure-result.json,pressure-result-first.json,pressure-resource-samples.json,pressure_test.go.txt,run-pressure.py,faults_test.go.txt}`;
logs under the adjacent `logs/` directory. The Go fixtures are supplied with `go test
-overlay` and are not part of routine CI. The runner enforces the stop thresholds.
All Go runs clear `CPA_API_KEY`; `TMPDIR` is under `/var/tmp`.

Standard regression checks: `gofmt -l .`, `env -u CPA_API_KEY go test ./...`,
`env -u CPA_API_KEY go test -race ./...`, and `env -u CPA_API_KEY go vet ./...`.
Mutating out entry eviction, restoring the old zero-byte background check, or
accepting over-budget history when only the protected alias remains makes the new
tests fail. The protected-only test also accepts one alias with `maxEntries=1`
without looping; if no evictable victim can satisfy a byte budget, admission returns
the existing budget error and rolls back. The final unmutated suite passes. Image/CI and rollout instructions
are in the lane's final `REPORT.md`; production deployment remains separate.
