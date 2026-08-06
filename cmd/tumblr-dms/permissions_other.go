//go:build !unix

package main

// Native Windows releases are not currently published. Keep the package
// buildable without pretending POSIX modes provide equivalent ACL protection.
func setSecureUmask() {}
