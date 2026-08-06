//go:build unix

package main

import "syscall"

func setSecureUmask() {
	syscall.Umask(0o077)
}
