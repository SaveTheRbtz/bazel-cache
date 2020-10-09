package utils

import (
	"go.uber.org/zap/zapcore"
)

type ZapLogLevelFlag zapcore.Level

func (f *ZapLogLevelFlag) Set(s string) error {
	return (*zapcore.Level)(f).Set(s)
}

func (f ZapLogLevelFlag) String() string {
	return (zapcore.Level)(f).String()
}

func (ZapLogLevelFlag) Type() string {
	return "zapcore.Level"
}
