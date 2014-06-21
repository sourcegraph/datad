package main

import (
	_ "expvar"
	"flag"
	"fmt"
	"log"
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
		fmt.Fprintln(os.Stderr, `datadctl - A command line client for datad.

Usage:

        datadctl [global options] command [options] [arguments...]

The commands are:
`)
		for _, c := range subcommands {
			fmt.Fprintf(os.Stderr, "    %-14s %s\n", c.Name, c.Description)
		}
		fmt.Fprintln(os.Stderr, `
Use "datadctl command -h" for more information about a command.

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
	c = datad.NewClient(b)

	subcmd := flag.Arg(0)
	extraArgs := flag.Args()[1:]
	for _, c := range subcommands {
		if c.Name == subcmd {
			c.Run(extraArgs)
			return
		}
	}

	fmt.Fprintf(os.Stderr, "datadctl: unknown subcommand %q\n", subcmd)
	fmt.Fprintln(os.Stderr, `Run "datadctl -h" for usage.`)
	os.Exit(1)
}

type subcommand struct {
	Name        string
	Description string
	Run         func(args []string)
}

var subcommands = []subcommand{
	{"list-providers", "list providers and their data URLs", listProvidersCmd},
	{"register-keys-on-providers", "scan provider for existing data and register it", registerKeysOnProvidersCmd},
	{"key", "print the provider/data URLs and versions for a key", keyCmd},
	{"help", "show this help message", func([]string) { flag.Usage() }},
}

var (
	c *datad.Client
)

func listProvidersCmd(args []string) {
	fs := flag.NewFlagSet("list-providers", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: datadctl list-providers [options]

List providers and their data URLs.

The options are:
`)
		fs.PrintDefaults()
		os.Exit(1)
	}
	fs.Parse(args)

	if fs.NArg() != 0 {
		fs.Usage()
	}

	providers, err := c.ListProviders()
	if err != nil {
		log.Fatal(err)
	}

	if len(providers) == 0 {
		fmt.Fprintln(os.Stderr, "# 0 providers found.")
		return
	}
	for _, p := range providers {
		fmt.Println(p)
	}
}

func keyCmd(args []string) {
	fs := flag.NewFlagSet("key", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: datadctl key [options] KEY

Print the provider/data URLs and versions for a key.

The options are:
`)
		fs.PrintDefaults()
		os.Exit(1)
	}
	fs.Parse(args)

	if fs.NArg() != 1 {
		fs.Usage()
	}

	key := fs.Arg(0)
	dvs, err := c.DataURLVersions(key)
	if err != nil {
		log.Fatal(err)
	}

	if len(dvs) == 0 {
		fmt.Fprintln(os.Stderr, "# 0 providers found for key.")
		return
	}
	for dataURL, ver := range dvs {
		fmt.Printf("%-10s %s\n", ver, dataURL)
	}
}

func registerKeysOnProvidersCmd(args []string) {
	fs := flag.NewFlagSet("key", flag.ExitOnError)
	fs.Usage = func() {
		fmt.Fprintln(os.Stderr, `usage: datadctl register-keys-on-providers [options] PROVIDER...

Registers keys for data that exists on provider. If no provider is specified,
this operation is performed on all providers.

The options are:
`)
		fs.PrintDefaults()
		os.Exit(1)
	}
	fs.Parse(args)

	providers := fs.Args()
	if len(providers) == 0 {
		var err error
		providers, err = c.ListProviders()
		if err != nil {
			log.Fatal(err)
		}
	}

	if *verbose {
		log.Printf("Registering keys on providers: %v", providers)
	}

	for _, p := range providers {
		err := c.RegisterKeysOnProvider(p)
		if err != nil {
			log.Fatalf("%s: %s", p, err)
		}
		fmt.Printf("Registered keys on provider %s\n", p)
	}
}
