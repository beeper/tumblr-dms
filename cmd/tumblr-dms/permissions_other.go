//go:build !unix

package main

import "os"

// Native Windows releases are not currently published. Keep the package
// buildable without pretending POSIX modes provide equivalent ACL protection.
func setSecureUmask() {}

func secureExistingFile(_ string, _ os.FileMode) error {
	return nil
}
