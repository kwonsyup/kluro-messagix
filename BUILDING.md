# Building kluro-messagix from this source

This tree is the Corresponding Source of `kluro-messagix`, the Messenger protocol sidecar. It builds one program, a
linux/amd64 binary at `/kluro-messagix` in a container image, with cgo (github.com/mattn/go-sqlite3).

Source repository: https://github.com/kwonsyup/kluro-messagix
Source offer and support contact: kwon@kluro.ai

## What fixes the bytes

- `Dockerfile` builds on `golang:1.26.10` pinned by digest
  (`sha256:672c944cfd07599b5f1bb179869270cdc94c6254e41db4d431259693cb286b82`, Debian 13, glibc 2.41).
  The C compiler and libc come from that image.
- `go.mod` and `go.sum` pin every module and its hash. `THIRD-PARTY.md` lists each module with its licence and the
  module-proxy URL of the exact source archive.
- The build command is
  `CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w -buildid=${REVISION}" -o /kluro-messagix .`
- Two build arguments name the build: `REVISION` (the full commit id) and `IMAGE_SOURCE` (the repository URL).

A build outside that image gives a working program with the same modules, but its bytes can differ.

## Build with Docker

    git archive --format=tar.gz -o src.tgz <commit>
    mkdir src && tar xzf src.tgz -C src
    docker build --platform=linux/amd64 \
      --build-arg REVISION=<full commit id> \
      --build-arg IMAGE_SOURCE=https://github.com/kwonsyup/kluro-messagix \
      --output type=local,dest=./out src
    sha256sum ./out/kluro-messagix

`--output` exports the files of the final stage, so `./out/kluro-messagix` is the binary. Use `-t <name>` instead of
`--output` to keep the image.

## Build with Google Cloud Build

`cloudbuild.yaml` builds from a `git archive` of the commit (never a working directory) and pushes an image.

    git archive --format=tar.gz -o src.tgz <commit>
    gcloud builds submit src.tgz --config cloudbuild.yaml --project <PROJECT> \
      --substitutions=_REGISTRY=<REGION>-docker.pkg.dev/<PROJECT>/<REPOSITORY>,_TAG=<tag>,_REVISION=<full commit id>,_SOURCE_URL=https://github.com/kwonsyup/kluro-messagix

The substitution defaults are placeholders and must be overridden. The result is `<REGISTRY>/kluro-messagix:<tag>`;
consumers should copy the binary at a pinned digest: `COPY --from=<image>@sha256:<digest> /kluro-messagix ...`.

## Verify a binary

Take the binary from the image (for example the single layer of the image, or `docker create` and `docker cp`), then:

    go tool buildid /kluro-messagix
    go version -m /kluro-messagix

- `go tool buildid` prints the commit the binary was built from (the `REVISION` build argument). The source is built
  without `.git`, so `go version -m` shows no `vcs.revision`; it shows the Go version (`go1.26.10`), the module path, the
  build flags (`-trimpath`, `CGO_ENABLED=1`) and every dependency with its version and hash, which match `go.sum`.
- The image labels name the commit and the repository: `org.opencontainers.image.revision` and
  `org.opencontainers.image.source` (read them with `docker inspect` or from the image config).
- To check that you built the same bytes, build the same commit with the same two build arguments and compare the
  binary's sha256 with the one published for that tag.

## Tests

    go test ./...

(needs cgo; the tests are not required to build the binary).

## Dependencies

To keep a copy of dependency source next to this tree:

    GOFLAGS=-mod=mod go mod vendor

## Running

The program speaks one JSON object per line on stdin and stdout (see `protocol.go`). It is not a network server.
