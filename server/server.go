package server

import (
	"context"
	"net"
	"os"

	pb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	_ "github.com/mostynb/go-grpc-compression/lz4"    // register lz4 support
	_ "github.com/mostynb/go-grpc-compression/snappy" // register snappy
	_ "github.com/mostynb/go-grpc-compression/zstd"   // register zstd support
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	_ "google.golang.org/grpc/encoding/gzip" // Register gzip support.
	"google.golang.org/grpc/reflection"

	"github.com/znly/bazel-cache/cache"
	_ "github.com/znly/bazel-cache/cache/disk"
	_ "github.com/znly/bazel-cache/cache/gcs"
	_ "github.com/znly/bazel-cache/cache/ipfs"
)

type cacheServer struct {
	cache *CacheEx
}

var serveCmdFlags = struct {
	listenAddr  string
	portFromEnv string
	cacheURI    string
}{}

func init() {
	ServeCmd.Flags().StringVarP(&serveCmdFlags.listenAddr, "port", "p", ":9092", "listen address")
	ServeCmd.Flags().StringVarP(&serveCmdFlags.portFromEnv, "port_from_env", "e", "", "get listen port from an environment variable")
	ServeCmd.Flags().StringVarP(&serveCmdFlags.cacheURI, "cache", "c", "", "cache uri")
}

var ServeCmd = &cobra.Command{
	Use:   "serve",
	Short: "Starts the Bazel cache gRPC server",
	RunE: func(cmd *cobra.Command, args []string) error {
		listenAddr := serveCmdFlags.listenAddr
		if serveCmdFlags.portFromEnv != "" {
			listenAddr = ":" + os.Getenv(serveCmdFlags.portFromEnv)
		}

		lis, err := net.Listen("tcp", listenAddr)
		if err != nil {
			return err
		}
		defer lis.Close()

		zap.L().With(
			zap.String("addr", lis.Addr().String()),
			zap.String("cache", serveCmdFlags.cacheURI),
		).Info("Listening")

		cc, err := cache.NewCacheFromURI(context.Background(), serveCmdFlags.cacheURI)
		if err != nil {
			return err
		}

		cs := &cacheServer{
			cache: NewCacheEx(cc),
		}

		grpcServer := grpc.NewServer(
			grpc.StreamInterceptor(grpc_middleware.ChainStreamServer(
				grpc_zap.StreamServerInterceptor(zap.L()),
			)),
			grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
				grpc_zap.UnaryServerInterceptor(zap.L()),
			)),
			grpc.ReadBufferSize(maxChunkSize),
			grpc.WriteBufferSize(maxChunkSize),
		)
		pb.RegisterActionCacheServer(grpcServer, cs)
		pb.RegisterCapabilitiesServer(grpcServer, cs)
		pb.RegisterContentAddressableStorageServer(grpcServer, cs)
		bytestream.RegisterByteStreamServer(grpcServer, cs)
		reflection.Register(grpcServer)

		return grpcServer.Serve(lis)
	},
}
