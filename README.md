# kluro-messagix

Messenger protocol sidecar for Kluro, licensed AGPL-3.0-or-later (see LICENSE
and NOTICE.md). The complete corresponding source is in this repository:
https://github.com/kwonsyup/kluro-messagix

Source offer and support: if you cannot use the repository, write to kwon@kluro.ai
and we will send you the complete corresponding source of the version you use, at no charge.

## Build

See BUILDING.md for the exact build, the build arguments and how to verify a binary. With Google Cloud Build:

    git archive --format=tar.gz -o src.tgz <commit>
    gcloud builds submit src.tgz --config cloudbuild.yaml --project <PROJECT> \
      --substitutions=_REGISTRY=<REGION>-docker.pkg.dev/<PROJECT>/<REPOSITORY>,_TAG=<tag>,_REVISION=<full commit id>,_SOURCE_URL=https://github.com/kwonsyup/kluro-messagix

Publishes `<REGISTRY>/kluro-messagix:<tag>` (the `_REGISTRY` value you passed)
with the binary at `/kluro-messagix` (linux/amd64, cgo, glibc 2.41 build). The image carries the
`org.opencontainers.image.source` and `org.opencontainers.image.revision` labels, and the binary's Go build id is the commit
(`go tool buildid /kluro-messagix`).
Consumers copy it at a pinned digest: `COPY --from=<image>@sha256:<digest> /kluro-messagix ...`.
