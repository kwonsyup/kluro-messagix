# Third-party components in the kluro-messagix binary

Generated from the Go build information of the deployed binary (`go version -m`) and the module
sources in the Go module cache. All 50 modules in go.mod are linked into the binary. License
texts shipped with each module are copied unmodified into `THIRD-PARTY-LICENSES/`.
The license column is a reviewed classification, not legal advice; counsel confirms.

Not covered here: the Go standard library and runtime (BSD-3-Clause, from the Go toolchain used to build), the SQLite
amalgamation inside mattn/go-sqlite3 (public domain), and the C library (glibc, LGPL-2.1+), which the binary loads dynamically from
the runtime image and which is not distributed in this source package.

| Module | Version | License | Source archive | go.sum h1 |
|---|---|---|---|---|
| filippo.io/edwards25519 | v1.2.0 | BSD-3-Clause | https://proxy.golang.org/filippo.io/edwards25519/@v/v1.2.0.zip | h1:crnVqOiS4jqYleHd9vaKZ+HKtHfllngJIiOpNpoJsjo= |
| github.com/andybalholm/brotli | v1.2.4 | MIT | https://proxy.golang.org/github.com/andybalholm/brotli/@v/v1.2.4.zip | h1:649LN6qF+/7JJYNN3Wdu4Dt5PS6eC/yP/Go3Bb0/AHI= |
| github.com/armfazh/h2c-go-ref | v0.0.0-20210215173008-07e12a6f8e0d | BSD-3-Clause | https://proxy.golang.org/github.com/armfazh/h2c-go-ref/@v/v0.0.0-20210215173008-07e12a6f8e0d.zip | h1:0YBiQ+rXap26i6apP7AldB+h50fAP7XSzpbio4ElXQo= |
| github.com/armfazh/tozan-ecc | v0.1.3 | BSD-3-Clause | https://proxy.golang.org/github.com/armfazh/tozan-ecc/@v/v0.1.3.zip | h1:g3OKE0KR4L/GZaoQYzsOUdg97Ey5lZRl1i1fD/QkUUw= |
| github.com/beeper/argo-go | v1.1.2 | MIT | https://proxy.golang.org/github.com/beeper/argo-go/@v/v1.1.2.zip | h1:UQI2G8F+NLfGTOmTUI0254pGKx/HUU/etbUGTJv91Fs= |
| github.com/beeper/poly1305 | v0.0.0-20250815183548-d4eede7bbf3c | BSD-3-Clause (no LICENSE file in the module; source files carry the Go Authors BSD-style header; same text as the golang.org/x/crypto LICENSE) | https://proxy.golang.org/github.com/beeper/poly1305/@v/v0.0.0-20250815183548-d4eede7bbf3c.zip | h1:Axyrzr6L42CLNJIQq0S87UhgK0Hq+4hCed4rc6sDdao= |
| github.com/bytemare/cryptotools | v0.0.0-20210309005813-f9985b05742f | MIT | https://proxy.golang.org/github.com/bytemare/cryptotools/@v/v0.0.0-20210309005813-f9985b05742f.zip | h1:CXLSVUN4laSSTqbZxZ2gCn5bqO+K8a4fhoru+92SQD0= |
| github.com/bytemare/opaque | v0.0.0-20210310201842-16a10d180656 | MIT | https://proxy.golang.org/github.com/bytemare/opaque/@v/v0.0.0-20210310201842-16a10d180656.zip | h1:zjeJZ8qK8Jzjx0Q4J51HoOpyuRcMxD3hZW5Ir4sV7y0= |
| github.com/bytemare/voprf | v0.0.0-20210221170502-f0af88d24d16 | MIT | https://proxy.golang.org/github.com/bytemare/voprf/@v/v0.0.0-20210221170502-f0af88d24d16.zip | h1:XRqQTKj1yBJZZ401bOiw/WDlF5BOfh+7OvPOXlfvfs4= |
| github.com/coder/websocket | v1.8.15 | ISC | https://proxy.golang.org/github.com/coder/websocket/@v/v1.8.15.zip | h1:6B2JPeOGlpff2Uz6vOEH1Vzpi0iUz20A+lPVhPHtNUA= |
| github.com/coreos/go-systemd/v22 | v22.7.0 | Apache-2.0 (plus NOTICE file) | https://proxy.golang.org/github.com/coreos/go-systemd/v22/@v/v22.7.0.zip | h1:LAEzFkke61DFROc7zNLX/WA2i5J8gYqe0rSj9KI28KA= |
| github.com/elliotchance/orderedmap/v3 | v3.1.0 | MIT | https://proxy.golang.org/github.com/elliotchance/orderedmap/v3/@v/v3.1.0.zip | h1:j4DJ5ObEmMBt/lcwIecKcoRxIQUEnw0L804lXYDt/pg= |
| github.com/google/go-querystring | v1.2.0 | BSD-3-Clause | https://proxy.golang.org/github.com/google/go-querystring/@v/v1.2.0.zip | h1:yhqkPbu2/OH+V9BfpCVPZkNmUXhb2gBxJArfhIxNtP0= |
| github.com/google/uuid | v1.6.0 | BSD-3-Clause | https://proxy.golang.org/github.com/google/uuid/@v/v1.6.0.zip | h1:NIvaJDMOsjHA8n1jAhLSgzrAzy1Hgr+hNrb57e+94F0= |
| github.com/gtank/ristretto255 | v0.2.0 | BSD-3-Clause | https://proxy.golang.org/github.com/gtank/ristretto255/@v/v0.2.0.zip | h1:LeOuWr6giplWkkMizx2emfG03SRPJqKt1nfIHLVHQ/0= |
| github.com/icholy/digest | v1.2.0 | MIT | https://proxy.golang.org/github.com/icholy/digest/@v/v1.2.0.zip | h1:oTbG4IsNOmidJ+421ehG7Ty93yt1yotq13kFMG569yw= |
| github.com/imroc/req/v3 (replace => github.com/beeper/req/v3 v3.0.0-20260925135712-b49593750e56) | v3.56.0 | MIT | https://proxy.golang.org/github.com/beeper/req/v3/@v/v3.0.0-20260925135712-b49593750e56.zip |  |
| github.com/klauspost/compress | v1.20.0 | Apache-2.0 (root); MIT and BSD-3-Clause for vendored sub-packages (internal/snapref, zstd/internal/xxhash) | https://proxy.golang.org/github.com/klauspost/compress/@v/v1.20.0.zip | h1:a3C1ke2ohxFymNlb2HWAHjDeKCI90scRskErZkR0ezA= |
| github.com/mattn/go-colorable | v0.1.15 | MIT | https://proxy.golang.org/github.com/mattn/go-colorable/@v/v0.1.15.zip | h1:+u9SLTRGnXv73cEsnsmoZBom+dMU88B2M0aDcWy0/jY= |
| github.com/mattn/go-isatty | v0.0.20 | MIT | https://proxy.golang.org/github.com/mattn/go-isatty/@v/v0.0.20.zip | h1:xfD0iDuEKnDkl03q4limB+vH+GxLEtL/jb4xVJSWWEY= |
| github.com/mattn/go-sqlite3 | v1.14.52 | MIT | https://proxy.golang.org/github.com/mattn/go-sqlite3/@v/v1.14.52.zip | h1:wVbm2Qnf4OXkqhBTSPuCRZDRnxfbVrrmiCEroVdog8U= |
| github.com/petermattis/goid | v0.0.0-20260918085751-abfca077860b | Apache-2.0 | https://proxy.golang.org/github.com/petermattis/goid/@v/v0.0.0-20260918085751-abfca077860b.zip | h1:OzNsuVdSWGwXvWKTtChx9ve89k4cFJ6NxYM4Aw4/f+E= |
| github.com/quic-go/qpack | v0.6.0 | MIT | https://proxy.golang.org/github.com/quic-go/qpack/@v/v0.6.0.zip | h1:g7W+BMYynC1LbYLSqRt8PBg5Tgwxn214ZZR34VIOjz8= |
| github.com/quic-go/quic-go | v0.61.0 | MIT | https://proxy.golang.org/github.com/quic-go/quic-go/@v/v0.61.0.zip | h1:ui88A53s8MSVYLC56en0KQ17HARk+9986Dn0SBfKNvA= |
| github.com/refraction-networking/utls | v1.8.2 | BSD-3-Clause | https://proxy.golang.org/github.com/refraction-networking/utls/@v/v1.8.2.zip | h1:j4Q1gJj0xngdeH+Ox/qND11aEfhpgoEvV+S9iJ2IdQo= |
| github.com/rs/xid | v1.6.0 | MIT | https://proxy.golang.org/github.com/rs/xid/@v/v1.6.0.zip | h1:fV591PaemRlL6JfRxGDEPl69wICngIQ3shQtzfy2gxU= |
| github.com/rs/zerolog | v1.35.1 | MIT | https://proxy.golang.org/github.com/rs/zerolog/@v/v1.35.1.zip | h1:m7xQeoiLIiV0BCEY4Hs+j2NG4Gp2o2KPKmhnnLiazKI= |
| github.com/tidwall/gjson | v1.20.0 | MIT | https://proxy.golang.org/github.com/tidwall/gjson/@v/v1.20.0.zip | h1:+agJ3rEzKcCXKDKo0ml26UROcpcAlnZyjq+TjbY5Fto= |
| github.com/tidwall/match | v1.1.1 | MIT | https://proxy.golang.org/github.com/tidwall/match/@v/v1.1.1.zip | h1:+Ho715JplO36QYgwN9PGYNhgZvoUSc9X2c80KVTi+GA= |
| github.com/tidwall/pretty | v1.2.1 | MIT | https://proxy.golang.org/github.com/tidwall/pretty/@v/v1.2.1.zip | h1:qjsOFOWWQl+N3RsoF5/ssm1pHmJJwhjlSbZ51I6wMl4= |
| github.com/tidwall/sjson | v1.2.5 | MIT | https://proxy.golang.org/github.com/tidwall/sjson/@v/v1.2.5.zip | h1:kLy8mja+1c9jlljvWTlSazM7cKDRfJuR/bOJhcY5NcY= |
| github.com/vektah/gqlparser/v2 | v2.5.27 | MIT | https://proxy.golang.org/github.com/vektah/gqlparser/v2/@v/v2.5.27.zip | h1:RHPD3JOplpk5mP5JGX8RKZkt2/Vwj/PZv0HxTdwFp0s= |
| github.com/vmihailenco/msgpack/v5 | v5.2.0 | BSD-2-Clause | https://proxy.golang.org/github.com/vmihailenco/msgpack/v5/@v/v5.2.0.zip | h1:ZhIAtVUP1mme8GIlpiAnmTzjSWMexA/uNF2We85DR0w= |
| github.com/vmihailenco/tagparser/v2 | v2.0.0 | BSD-2-Clause | https://proxy.golang.org/github.com/vmihailenco/tagparser/v2/@v/v2.0.0.zip | h1:y09buUbR+b5aycVFQs/g70pqKVZNBmxwAhO7/IwNM9g= |
| github.com/yuin/goldmark | v1.8.6 | MIT | https://proxy.golang.org/github.com/yuin/goldmark/@v/v1.8.6.zip | h1:d0VcaP1sx9GkFVkoW+KtggpGi2KZ965i14b0+bDQST4= |
| go.mau.fi/libsignal | v0.2.2 | GPL-3.0 (license text is GPL version 3; no per-file or-later grant found) | https://proxy.golang.org/go.mau.fi/libsignal/@v/v0.2.2.zip | h1:QV+XdzQkm3x3aSG7FcqfGSZuFXz83pRZPBFaPygHbOU= |
| go.mau.fi/mautrix-meta | v0.2610.1-0.20261024105111-2e7f46f1597a | AGPL-3.0-or-later (plus LICENSE.exceptions: grants to Beeper and Element only, none to Kluro) | https://proxy.golang.org/go.mau.fi/mautrix-meta/@v/v0.2610.1-0.20261024105111-2e7f46f1597a.zip | h1:DNUSel7Lv+1ZXcpsPBBI3wBx6jOhAgwT/I4zUH9uCxg= |
| go.mau.fi/util | v0.11.0 | MPL-2.0 | https://proxy.golang.org/go.mau.fi/util/@v/v0.11.0.zip | h1:JNEpkVRxfTdJUC0DIGKiRlLKzl4G/qLX/eVzoP7QuwY= |
| go.mau.fi/whatsmeow | v0.0.0-20261014094007-f16f9ca30d6a | MPL-2.0 | https://proxy.golang.org/go.mau.fi/whatsmeow/@v/v0.0.0-20261014094007-f16f9ca30d6a.zip | h1:33FqqrtRrtYYieuzwp9tufGiPJJjrqES+DUvOHbQWZE= |
| go.mau.fi/zeroconfig | v0.2.0 | MPL-2.0 | https://proxy.golang.org/go.mau.fi/zeroconfig/@v/v0.2.0.zip | h1:e/OGEERqVRRKlgaro7E6bh8xXiKFSXB3eNNIud7FUjU= |
| golang.org/x/crypto | v0.58.0 | BSD-3-Clause; UNCLASSIFIED | https://proxy.golang.org/golang.org/x/crypto/@v/v0.58.0.zip | h1:COkYLr4k7nDI4r6QtMML2AfzeIswWHAJthLS6K/M/54= |
| golang.org/x/exp | v0.0.0-20261014143641-f0251eb4aa3e | BSD-3-Clause; UNCLASSIFIED | https://proxy.golang.org/golang.org/x/exp/@v/v0.0.0-20261014143641-f0251eb4aa3e.zip | h1:HgI6W1OolKK6tcV74is+rl9SMT0njsHlp0TbQiGXI/A= |
| golang.org/x/net | v0.61.0 | BSD-3-Clause; UNCLASSIFIED | https://proxy.golang.org/golang.org/x/net/@v/v0.61.0.zip | h1:RR+6j/BTBCrggOLugxa+66R8FvoN6MHlzDlZIbN9YFw= |
| golang.org/x/sync | v0.24.0 | BSD-3-Clause; UNCLASSIFIED | https://proxy.golang.org/golang.org/x/sync/@v/v0.24.0.zip | h1:iqiDrGoDgyuvAKrhLQthUEzU/R378N78gXbBjxed7oA= |
| golang.org/x/sys | v0.49.0 | BSD-3-Clause; UNCLASSIFIED | https://proxy.golang.org/golang.org/x/sys/@v/v0.49.0.zip | h1:XbzkgYJHdqh/8m2Uu0W/dQv8nktxx4BFHp1M0gROTXA= |
| golang.org/x/text | v0.43.0 | BSD-3-Clause; UNCLASSIFIED | https://proxy.golang.org/golang.org/x/text/@v/v0.43.0.zip | h1:1QivrlhwAsnMOcqgOdl9my7s9OyZl8O5/UGUTJVXoWc= |
| google.golang.org/protobuf | v1.36.12 | BSD-3-Clause; UNCLASSIFIED | https://proxy.golang.org/google.golang.org/protobuf/@v/v1.36.12.zip | h1:pJOKDDOyeXErUroCihFAd5LQuwXBSpVnKGrj5o/fwxc= |
| gopkg.in/natefinch/lumberjack.v2 | v2.2.1 | MIT | https://proxy.golang.org/gopkg.in/natefinch/lumberjack.v2/@v/v2.2.1.zip | h1:bBRl1b0OH9s/DuPhuXpNl+VtCaJXFZ5/uEFST95x9zc= |
| gopkg.in/yaml.v3 | v3.0.1 | Apache-2.0 and MIT (LICENSE), plus NOTICE | https://proxy.golang.org/gopkg.in/yaml.v3/@v/v3.0.1.zip | h1:fxVm/GzAzEWqLHuvctI91KS9hhNmmWOoWu0XTYJS7CA= |
| maunium.net/go/mautrix | v0.32.0 | MPL-2.0 | https://proxy.golang.org/maunium.net/go/mautrix/@v/v0.32.0.zip | h1:xuAZzzyWce0pFkBhV/MCLFiJdVs2rFkY3Eep7hNng4I= |

## Notes for counsel

- **go.mau.fi/mautrix-meta** (AGPL-3.0-or-later) is the upstream this program is built on. Its `LICENSE.exceptions` grants extra rights to Beeper and Element only. Kluro holds no exception, so the program and any binary of it are conveyed under AGPL-3.0-or-later with no exception.
- **go.mau.fi/libsignal** is GPL-3.0 and is linked into the binary. GPL-3.0 section 13 and AGPL-3.0 section 13 allow combining the two; the combined work's network-use clause is the AGPL's.
- **go.mau.fi/whatsmeow, go.mau.fi/util, go.mau.fi/zeroconfig, maunium.net/go/mautrix** are MPL-2.0 (file-level copyleft). Modifications to those files, if any, must stay under MPL-2.0. This package modifies none of them; they are consumed unmodified from the module proxy.
- **github.com/imroc/req/v3** is replaced by the fork `github.com/beeper/req/v3` (MIT) through a `replace` directive in go.mod.
- **github.com/beeper/poly1305** ships no license file; see the table row.
