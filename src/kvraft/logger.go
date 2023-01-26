package kvraft

import (
	"fmt"

	"6.824-2022/logger"
)

func (kv *KVServer) Out(format string, args ...interface{}) {
	logger.Out.Output(2, fmt.Sprintf("[KV-%v](S%v) %v\n", kv.rf.GetStateString(), kv.me, fmt.Sprintf(format, args...)))
}

func (kv *KVServer) Debug(format string, args ...interface{}) {
	logger.Debug.Output(2, fmt.Sprintf("[KV-%v](S%v) %v\n", kv.rf.GetStateString(), kv.me, fmt.Sprintf(format, args...)))
}

func (kv *KVServer) Warn(format string, args ...interface{}) {
	logger.Warn.Output(2, fmt.Sprintf("[KV-%v](S%v) %v\n", kv.rf.GetStateString(), kv.me, fmt.Sprintf(format, args...)))
}

func (kv *KVServer) Error(format string, args ...interface{}) {
	logger.Error.Output(2, fmt.Sprintf("[KV-%v](S%v) %v\n", kv.rf.GetStateString(), kv.me, fmt.Sprintf(format, args...)))
}

func (kv *KVServer) Verbose(format string, args ...interface{}) {
	logger.Verbose.Output(2, fmt.Sprintf("[KV-%v](S%v) %v\n", kv.rf.GetStateString(), kv.me, fmt.Sprintf(format, args...)))
}
