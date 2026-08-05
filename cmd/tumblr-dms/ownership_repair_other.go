//go:build !unix

package main

import "fmt"

func repairSQLiteOwnership(string, string, int, int) error {
	return fmt.Errorf("SQLite ownership repair is only supported on Unix")
}
