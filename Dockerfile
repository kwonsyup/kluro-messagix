# Build context is the repository root.
# cgo (mattn/go-sqlite3) links the build stage's glibc. The connector runtime
# (chromedp/headless-shell, Debian 13, glibc 2.41) must provide the same or
# newer, so build on the same golang image the connector uses (Debian 13).
# The Go image is pinned by digest so the same source builds the same binary.
#
# Build arguments (cloudbuild.yaml passes them):
#   REVISION      the full commit id of the source being built; recorded in the
#                 image label and as the binary's Go build id (the source is
#                 built without .git, so there is no vcs.revision), read with
#                 `go tool buildid /kluro-messagix`.
#   IMAGE_SOURCE  URL of the source repository; recorded in the image label.
FROM --platform=linux/amd64 golang:1.26.10@sha256:672c944cfd07599b5f1bb179869270cdc94c6254e41db4d431259693cb286b82 AS build
ARG REVISION=unknown
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags="-s -w -buildid=${REVISION}" -o /kluro-messagix .

# Carrier image only: the connector build copies the binary out of it at a
# pinned digest. It is not meant to run.
FROM scratch
ARG REVISION=unknown
ARG IMAGE_SOURCE=SOURCE_URL_PLACEHOLDER
LABEL org.opencontainers.image.source="${IMAGE_SOURCE}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.licenses="AGPL-3.0-or-later"
COPY --from=build /kluro-messagix /kluro-messagix
