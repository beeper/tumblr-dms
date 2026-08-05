//go:build unix

package main

import (
	"fmt"
	"os"
	"syscall"
)

func setSecureUmask() {
	syscall.Umask(0o077)
}

func secureExistingFile(path string, mode os.FileMode) error {
	info, err := os.Lstat(path)
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("expected a regular file, found %s", info.Mode().Type())
	}

	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	openedInfo, err := file.Stat()
	if err != nil {
		return err
	}
	if !os.SameFile(info, openedInfo) {
		return fmt.Errorf("path changed while securing it")
	}
	if err = file.Chmod(mode); err != nil {
		return err
	}
	if err = verifyMode(file, mode); err != nil {
		return err
	}
	return verifyPathStillMatches(path, openedInfo)
}

func verifyMode(file *os.File, expected os.FileMode) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if actual := info.Mode().Perm(); actual != expected.Perm() {
		return fmt.Errorf("permissions are %04o after chmod, expected %04o", actual, expected.Perm())
	}
	if info.Mode()&(os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return fmt.Errorf("special permission bits remain after chmod")
	}
	return nil
}

func verifyPathStillMatches(path string, openedInfo os.FileInfo) error {
	pathInfo, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !os.SameFile(openedInfo, pathInfo) {
		return fmt.Errorf("path changed while securing it")
	}
	return nil
}
