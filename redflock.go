package main

import (
	"flag"
	"fmt"
	"github.com/hjr265/redsync.go/redsync"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
)

var logger = log.New(os.Stderr, "redflock:", log.LstdFlags)

var DEBUG bool
var QUIET bool

func debug(params ...interface{}) {
	if DEBUG {
		logger.Println(params...)
	}
}

func info(params ...interface{}) {
	if !QUIET {
		logger.Println(params...)
	}
}

func fatal(params ...interface{}) {
	logger.Fatalln(params...)
}

func main() {
	flag.BoolVar(&DEBUG, "debug", false, "enable debug logging")
	flag.BoolVar(&QUIET, "quiet", false, "disable output")
	addresses := flag.String("address", "127.0.0.1:6379", "redis host and port (or multiple comma-separated)")
	ttl := flag.Int("ttl", 60, "lock duration (seconds)")
	tries := flag.Int("tries", 15, "number of tries to acquire lock")
	delay := flag.Int("delay", 5, "number of seconds between tries")
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: redflock [options] lock-name command\n\n")
		fmt.Fprintf(os.Stderr, "e.g. redflock -address redis01:6379,redis02:6379 my-cmd-lock my-cmd\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()

	args := flag.Args()
	if len(args) < 2 {
		fatal("lock-name and command required")
	}

	redlock := getRedlock(args[0], *addresses, *ttl, *tries, *delay)
	debug("acquiring:", args[0])
	if err := redlock.Lock(); err != nil {
		fatal(err)
	}
	info("acquired:", args[0])

	var wg sync.WaitGroup
	var exitCode int
	done := make(chan bool, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		exitCode = cmdAgent(exec.Command(args[1], args[2:]...), done)
	}()
	go func() {
		defer wg.Done()
		lockAgent(redlock, done)
	}()

	wg.Wait()
	os.Exit(exitCode)
}

func lockAgent(redlock *redsync.Mutex, done chan bool) {
	defer redlock.Unlock()
	timer := time.NewTicker(redlock.Expiry / 2)
	for {
		select {
		case <-timer.C:
			debug("touching:", redlock.Name)
			ok := redlock.Touch()
			if !ok {
				info("touch failed")
				done <- true
				return
			}
		case <-done:
			debug("releasing:", redlock.Name)
			return
		}
	}
}

func cmdAgent(cmd *exec.Cmd, done chan bool) int {
	exitCode := make(chan int)
	go func() {
		exitCode <- supervise(cmd)
	}()

	select {
	case code := <-exitCode:
		done <- true
		return code
	case <-done:
		if err := cmd.Process.Kill(); err != nil {
			fatal("failed to kill child:", err)
		}
		debug("killed child")
		return 1
	}
}

func supervise(cmd *exec.Cmd) int {
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	debug("starting child:", cmd.Path, cmd.Args)
	if err := cmd.Start(); err != nil {
		info(err)
		return 1
	}

	exitCode := make(chan int)
	go func() {
		if err := cmd.Wait(); err != nil {
			debug("child exited:", err)
			exitCode <- getExitStatus(err)
		} else {
			debug("child exited successfully")
			exitCode <- 0
		}
	}()

	signals := make(chan os.Signal, 10)
	signal.Notify(signals, syscall.SIGHUP, syscall.SIGINT, syscall.SIGKILL, syscall.SIGQUIT, syscall.SIGSTOP, syscall.SIGTERM, syscall.SIGUSR1, syscall.SIGUSR2)

	for {
		select {
		case code := <-exitCode:
			return code
		case sig := <-signals:
			debug("forwarding signal:", sig)
			if err := cmd.Process.Signal(sig); err != nil {
				info("failed to forward signal:", sig, err)
			}
		}
	}
}

func getExitStatus(err error) int {
	// http://stackoverflow.com/a/10385867
	if exiterr, ok := err.(*exec.ExitError); ok {
		// The program has exited with an exit code != 0

		// This works on both Unix and Windows. Although package
		// syscall is generally platform dependent, WaitStatus is
		// defined for both Unix and Windows and in both cases has
		// an ExitStatus() method with the same signature.
		if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
			return status.ExitStatus()
		}
	}
	return 1
}

func parseAddresses(addresses string) []net.Addr {
	addrs := strings.Split(addresses, ",")
	ret := make([]net.Addr, len(addrs))
	for i, address := range addrs {
		addr, err := net.ResolveTCPAddr("tcp", address)
		if err != nil {
			fatal("couldn't parse or resolve", address)
		}
		ret[i] = addr
	}
	debug("servers:", ret)
	return ret
}

func getRedlock(name string, addresses string, ttl int, tries int, delay int) *redsync.Mutex {
	m, err := redsync.NewMutex(name, parseAddresses(addresses))
	if err != nil {
		fatal(err)
	}
	m.Delay = time.Duration(delay) * time.Second
	m.Tries = tries
	m.Expiry = time.Duration(ttl) * time.Second
	return m
}
