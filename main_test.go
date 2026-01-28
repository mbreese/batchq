package main

import (
	"os"
	"testing"
)

func TestMainCommand(t *testing.T) {
	oldArgs := os.Args
	os.Args = []string{"batchq", "license"}
	defer func() { os.Args = oldArgs }()

	main()
}
