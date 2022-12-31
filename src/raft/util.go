package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const isDebug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if isDebug {
		log.Printf(format, a...)
	}
	return
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func randomTimeout() <-chan time.Time {
	period := 300*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
	return time.After(period)
}
