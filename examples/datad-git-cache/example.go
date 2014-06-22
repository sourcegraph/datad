package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/coreos/go-etcd/etcd"
	"github.com/sourcegraph/datad"
)

var (
	verbose       = flag.Bool("v", true, "show verbose output")
	etcdEndpoint  = flag.String("etcd", "http://127.0.0.1:4001", "etcd endpoint")
	etcdKeyPrefix = flag.String("etcd-key-prefix", datad.DefaultKeyPrefix, "keyspace for datad registry and provider list in etcd")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, `datad-git-cache: An example program demonstrating datad.

Usage:

        datad-git-cache [global options] command [options] [arguments...]

The commands are:
`)
		for _, c := range subcommands {
			fmt.Fprintf(os.Stderr, "    %-14s %s\n", c.Name, c.Description)
		}
		fmt.Fprintln(os.Stderr, `
Use "datad-git-cache command -h" for more information about a command.

The global options are:
`)
		flag.PrintDefaults()
		os.Exit(1)
	}
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
	}

	b := datad.NewEtcdBackend(*etcdKeyPrefix, etcd.NewClient([]string{*etcdEndpoint}))
	c = datad.NewClient(b, "/")

	subcmd := flag.Arg(0)
	extraArgs := flag.Args()[1:]
	for _, c := range subcommands {
		if c.Name == subcmd {
			c.Run(extraArgs)
			return
		}
	}

	fmt.Fprintf(os.Stderr, "datad-git-cache: unknown subcommand %q\n", subcmd)
	fmt.Fprintln(os.Stderr, `Run "datad-git-cache -h" for usage.`)
	os.Exit(1)
}

type subcommand struct {
	Name        string
	Description string
	Run         func(args []string)
}

var subcommands = []subcommand{
	{"serve", "start a server", serveCmd},
	{"help", "show this help message", func([]string) { flag.Usage() }},
}

var (
	c *datad.Client
)

func serveCmd(args []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: datad-git-cache serve [options]

Start a git cache server and register it as a datad provider.

The git cache server has an HTTP API that returns the contents of files in a git
repository.

The options are:
`)
		fs.PrintDefaults()
		os.Exit(1)
	}
	fs.Parse(args)

	if fs.NArg() != 0 {
		fs.Usage()
	}

}
