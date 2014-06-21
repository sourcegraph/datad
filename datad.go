package datad

import (
	"log"
	"os"
	"strings"
)

const (
	version          = "0.0.1"
	DefaultKeyPrefix = "/datad/"
)

var (
	Log = log.New(os.Stderr, "datad: ", log.Ltime|log.Lshortfile)
)

// slash adds a leading slash if path does not contain one.
func slash(path string) string {
	if len(path) == 0 {
		return "/"
	} else if path[0] == '/' {
		return path
	}
	return "/" + path
}

// unslash removes a leading slash from path if it contains one.
func unslash(path string) string {
	return strings.TrimPrefix(path, "/")
}
