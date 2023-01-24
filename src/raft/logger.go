package raft

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/fatih/color"
)

var Out *log.Logger
var Error *log.Logger
var Warn *log.Logger
var Debug *log.Logger
var Verbose *log.Logger
var logger *os.File
var isVerbose bool = false

var yellow func(a ...interface{}) string
var red func(a ...interface{}) string
var blue func(a ...interface{}) string

func init() {
	path, err := filepath.Abs("./logs")
	if err != nil {
		fmt.Println("Error reading given path: ", err.Error())
	}

	logger, _ = os.OpenFile(fmt.Sprintf("%v/logs", path), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)

	// mwOut := io.MultiWriter(os.Stdout, logger)
	mwOut := io.MultiWriter(ioutil.Discard, logger)
	// mwErr := io.MultiWriter(os.Stderr, logger)
	mwErr := io.MultiWriter(ioutil.Discard, logger)
	mwDebug := io.MultiWriter(ioutil.Discard)
	mwVerbose := io.MultiWriter(ioutil.Discard)

	yellow = color.New(color.FgYellow).SprintFunc()
	red = color.New(color.FgRed).SprintFunc()
	blue = color.New(color.FgBlue).SprintFunc()

	Out = log.New(mwOut, "OUT: ", log.Ltime|log.Lshortfile)
	Error = log.New(mwErr, red("ERROR: "), log.Ltime|log.Lshortfile)
	Warn = log.New(mwErr, yellow("WARN: "), log.Ltime|log.Lshortfile)
	Debug = log.New(mwDebug, blue("DEBUG: "), log.Ltime|log.Lshortfile)
	Verbose = log.New(mwVerbose, blue("VERBOSE: "), log.Ltime|log.Lshortfile)
}

func SetDebug(enabled bool) {
	if enabled {
		mwDebug := io.MultiWriter(os.Stdout, logger)
		Debug.SetOutput(mwDebug)
	} else {
		Debug.SetOutput(ioutil.Discard)
	}
}
func SetVerbose(enabled bool) {
	isVerbose = enabled
	if enabled {
		mwVerbose := io.MultiWriter(os.Stdout, logger)
		Verbose.SetOutput(mwVerbose)
	} else {
		Verbose.SetOutput(ioutil.Discard)
	}
}

func SuppressLogs() {
	Verbose.SetOutput(ioutil.Discard)
	Debug.SetOutput(ioutil.Discard)
	Out.SetOutput(ioutil.Discard)
	Warn.SetOutput(ioutil.Discard)
	Error.SetOutput(ioutil.Discard)
}

func (rf *Raft) Out(format string, args ...interface{}) {
	Out.Output(2, fmt.Sprintf("(%v/%v/T%v) %v\n", rf.me, rf.getStateString(), rf.currentTerm.Load(), fmt.Sprintf(format, args...)))
}

func (rf *Raft) Debug(format string, args ...interface{}) {
	color.Set(color.FgBlue)
	Debug.Output(2, blue(fmt.Sprintf("(%v/%v/T%v) %v\n", rf.me, rf.getStateString(), rf.currentTerm.Load(), fmt.Sprintf(format, args...))))
	color.Unset()
}
func (rf *Raft) Verbose(format string, args ...interface{}) {
	color.Set(color.FgBlue)
	Verbose.Output(2, blue(fmt.Sprintf("(%v/%v/T%v) %v\n", rf.me, rf.getStateString(), rf.currentTerm.Load(), fmt.Sprintf(format, args...))))
	color.Unset()
}

func (rf *Raft) Error(format string, args ...interface{}) {
	color.Set(color.FgRed)
	Error.Output(2, red(fmt.Sprintf("(%v/%v/T%v) %v\n", rf.me, rf.getStateString(), rf.currentTerm.Load(), fmt.Sprintf(format, args...))))
	color.Unset()
}

func (rf *Raft) Warn(format string, args ...interface{}) {
	color.Set(color.FgYellow)
	Warn.Output(2, yellow(fmt.Sprintf("(%v/%v/T%v) %v\n", rf.me, rf.getStateString(), rf.currentTerm.Load(), fmt.Sprintf(format, args...))))
	color.Unset()
}
