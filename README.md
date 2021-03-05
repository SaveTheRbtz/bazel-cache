# bazel-cache
znly/bazel-cache is a minimal, cloud oriented Bazel remote cache.

It only supports Bazel's v2 remote execution protocol over gRPC.

It is meant to be deployed serverless-ly (currently on Cloud Run) and backed by an object storage (currently Google Cloud Storage). Of course PRs for other platforms are welcomed. Thanks to Google Cloud Storage Object Lifecycle Management, it features automatic TTL-based garbage collection.

It was inspired by [buchgr/bazel-remote](https://github.com/buchgr/bazel-remote)'s simplicity and accessibility.

# Usage
To start the server, simply run:
```
$ bazel-cache serve --help
Starts the Bazel cache gRPC server

Usage:
  bazel-cache serve [flags]

Flags:
  -c, --cache string           cache uri
  -h, --help                   help for serve
  -p, --port string            listen address (default ":9092")
  -e, --port_from_env string   get listen port from an environment variable

Global Flags:
  -l, --loglevel zapcore.Level   Log Level (default info)

$ bazel-cache serve -c gcs://MYBUCKET?ttl_days=14
2021-02-28T21:33:41.932+0100	INFO	server/server.go:59	Listening	{"addr": "[::]:9092", "cache": "gcs://MYBUCKET?ttl_days=14"}
```

Currently only `gcs://` and `file://` URIs are supported.

There are a few options behind `--help`.

## Bazel configuration
Simply add the following on the `.bazelrc`
```
build:cache             --remote_download_minimal
build:cache             --remote_cache=grpcs://MY-CLOUD-RUN-SERVICE.a.run.app
```

Copy the supplied `tools/bazel` to `tools/bazel` in the workspace. Then modify with the correct Cloud Run URL:
```python
CACHE_URL = "https://MY-CLOUD-RUN-SERVICE.a.run.app"
```

Now, simply add `--config=cache` to the any bazel command.

# Bazel gRPC remote caching protocol
Bazel implements remote caching over several protocols: HTTP/WebDAV, gRPC and Google Cloud Storage. We went with the [v2 remote execution over gRPC](https://github.com/bazelbuild/remote-apis/blob/master/build/bazel/remote/execution/v2/remote_execution.proto). It is a lot faster than the legacy ones, especially when combined with `--remote_download_toplevel` a.k.a [Remote Builds without the Bytes](https://github.com/bazelbuild/bazel/issues/6862). Additionally, because gRPC is really HTTP/2, it is a lot less constrained by geographic imperatives for throughput based applications.

As an example, here is a fully cached build of `@com_github_apple_swift_protobuf//:SwiftProtobuf` with gRPC and HTTPS backed by the same GCS bucket on Bazel 4.0.0:

### gRPC
```

$ bazel build @com_github_apple_swift_protobuf//:SwiftProtobuf --remote_cache=grpcs://mybazelcache-xxxx.xxxx.run.app --remote_download_minimal --remote_max_connections=30 --remote_timeout=30
Target @com_github_apple_swift_protobuf//:SwiftProtobuf up-to-date:
  bazel-bin/external/com_github_apple_swift_protobuf/SwiftProtobuf-Swift.h
  bazel-bin/external/com_github_apple_swift_protobuf/SwiftProtobuf.swiftdoc
  bazel-bin/external/com_github_apple_swift_protobuf/SwiftProtobuf.swiftmodule
  bazel-bin/external/com_github_apple_swift_protobuf/libSwiftProtobuf.a
INFO: Elapsed time: 4.471s, Critical Path: 1.40s
INFO: 136 processes: 122 remote cache hit, 14 internal.
INFO: Build completed successfully, 136 total actions
```
### HTTPS
```
$ $ bazel build @com_github_apple_swift_protobuf//:SwiftProtobuf --remote_cache=https://storage.googleapis.com/MYBUCKET --remote_download_minimal --remote_max_connections=30 --remote_timeout=30
Target @com_github_apple_swift_protobuf//:SwiftProtobuf up-to-date:
  bazel-bin/external/com_github_apple_swift_protobuf/SwiftProtobuf-Swift.h
  bazel-bin/external/com_github_apple_swift_protobuf/SwiftProtobuf.swiftdoc
  bazel-bin/external/com_github_apple_swift_protobuf/SwiftProtobuf.swiftmodule
  bazel-bin/external/com_github_apple_swift_protobuf/libSwiftProtobuf.a
INFO: Elapsed time: 10.455s, Critical Path: 3.09s
INFO: 136 processes: 122 remote cache hit, 14 internal.
INFO: Build completed successfully, 136 total actions
```

The reason it's faster is mostly due to how the gRPC remote protocol works vs HTTP and znly/bazel-cache spawns multiple calls to GCS in parallel on a per-file basis. Also, Cloud Run is much closer to GCS that our CI or developer machines are.

# Automatic Garbage Collection
The `gcs://` URI can take a `ttl_days` parameter to enable automatic garbage collection. It is enabled via [Google Cloud Storage Object Lifecycle Management](https://cloud.google.com/storage/docs/lifecycle) by setting a `DaysSinceCustomTime` + `DeleteAction` on the bucket when it is started.

When a hash is looked up by Bazel, `znly/bazel-cache` will determine if the hash exists by updating its `CustomTime`. This means that testing if an object exists (and fetching its metadata such as size) and bumping its `CustomTime` happens in a single pass.
Because the `CustomTime` is bumped when testing if an object exists, only objects that haven't been looked up in a won't get their `CustomTime` updated, and thus will be deleted by GCS. Effectively enabling  garbage collection for free. This is controlled by the `ttl_days` URL parameter.

# Deploying on Google Cloud Run
When deployed on Google Cloud Run, it will handle TLS, load balancing, scaling (down to zero) and authentication. This is standard Cloud Run really, but here goes:

## Creating the GCS Bucket
First, create the GCS bucket in the same region your plan to deploy the container to (for optimised latency). We found the STANDARD class in a single region to be the best performing (YMMV):
```
gsutil mb gs://MYBUCKET \
    -p MYPROJECT \
    -c STANDARD \   # standard is fastest
    -l REGION \
    -b on           # use bucket-wide ACLs
```

## Creating a dedicated Service Account
Create a service account that has the proper permissions to write to the bucket and administer it by adding the necessary roles to it:
```
gcloud iam service-accounts create bazel-cache --project MYPROJECT

ROLES=(run.invoker storage.objectCreator storage.objectViewer)
for role in ${ROLES}; do
    gcloud projects add-iam-policy-binding MYPROJECT \
        --member=serviceAccount:bazel-cache@MYPROJECT.iam.gserviceaccount.com \
        --role=roles/${role}
done
```

## Pushing the Image
Pull the znly/bazel-cache image and push it to your project'sgcr.io registry:
```
docker pull znly/bazel-cache:0.0.3
docker tag znly/bazel-cache:0.0.3 gcr.io/MYPROJECT/bazel-cache:0.0.3
docker push gcr.io/MYPROJECT/bazel-cache:0.0.3
```

## Deploying the service
Once everything is done, deploy thebazel-cache service with the proper service account:
```
gcloud run deploy bazel-cache \
    --service-account=bazel-cache@MYPROJECT.iam.gserviceaccount.com \
    --project=MYPROJECT \
    --region=REGION \
    --platform=managed \
    --port=9092 \
    --cpu=4 \
    --memory=8Gi \
    '--args=serve,--loglevel,INFO,--port,:9092,--cache,gcs://MYBUCKET?ttl_days=30' \
    --image=gcr.io/MYPROJECT/bazel-cache:0.0.3 \
    --concurrency=80
```

The service should appear in your Cloud Run console. It's done! `bazel-cache` is running in Cloud Run.

# Authentication
This one is tricky at the moment. Cloud Run has builtin authentication, and it's perfect for that use case.

However, while Bazel supports Google Cloud's Access Tokens via the `--google_credentials` and `--google_default_credentials` flags, Cloud Run only supports Identity Tokens as of the writing of this README. Bazel could support Identity Tokens natively, but it is not possible until [googleapis/google-auth-library-java#469](https://github.com/googleapis/google-auth-library-java/pull/469) and [bazelbuild/bazel#12135](https://github.com/bazelbuild/bazel/issues/12135) are fixed.

Thankfully, Bazel's builtin wrapper support makes it possible to invoke `gcloud auth print-identity-token` and pass the token via the `--remote_header` flag: when invoked, Bazel will look for an executable at `tools/bazel` in the workspace and, if it exists, will invoke it with a `$BAZEL_REAL` environment pointing the actual Bazel binary.

You'll need to edit the supplied `tools/bazel` wrapper (made in python) for you custom Cloud Run URL. The script will look for a `--config=cache` flag and invoke `gcloud` as needed. This is a tiny bit hackish, but it works well enough for now. Until native support in Bazel or Cloud Run, that is.
