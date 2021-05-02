package main

import (
	"fmt"

	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/znly/bazel-cache/server"
	"github.com/znly/bazel-cache/utils"
)

var globalFlags = struct {
	loglevel zapcore.Level
}{}

func init() {
	rootCmd.PersistentFlags().VarP((*utils.ZapLogLevelFlag)(&globalFlags.loglevel), "loglevel", "l", "Log Level")

	rootCmd.AddCommand(server.ServeCmd)
}

var rootCmd = &cobra.Command{
	Use:   "bazel-cache",
	Short: "Minimal cloud oriented Bazel cache",
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		logConfig := zap.NewProductionConfig()
		logConfig.DisableCaller = true

		logConfig.Level = zap.NewAtomicLevelAt(globalFlags.loglevel)
		logger, err := logConfig.Build()
		if err != nil {
			return fmt.Errorf("unable to create logger: %w", err)
		}
		defer logger.Sync() // flushes buffer, if any
		zap.ReplaceGlobals(logger)
		return nil
	},
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}
