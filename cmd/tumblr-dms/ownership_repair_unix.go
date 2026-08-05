//go:build unix

package main

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"golang.org/x/sys/unix"
)

var tumblrCoordinationLockName = regexp.MustCompile(
	`^\.tumblr-(?:portal-reid|outbound-submission)-[0-9a-f]{64}\.lock$`,
)

func repairSQLiteOwnership(dataDir, databasePath string, uid, gid int) error {
	if !filepath.IsAbs(databasePath) {
		databasePath = filepath.Join(dataDir, databasePath)
	}
	databasePath = filepath.Clean(databasePath)
	if !pathInsideDirectory(dataDir, databasePath) {
		return fmt.Errorf("SQLite database %q must be inside the data directory", databasePath)
	}
	relativePath, err := filepath.Rel(dataDir, databasePath)
	if err != nil || relativePath == "." {
		return fmt.Errorf("resolve SQLite database beneath data directory")
	}
	components := strings.Split(relativePath, string(filepath.Separator))
	for _, component := range components {
		if component == "" || component == "." || component == ".." {
			return fmt.Errorf("SQLite database path contains an unsafe component")
		}
	}

	dataFD, err := unix.Open(dataDir, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return fmt.Errorf("open data directory without following links: %w", err)
	}
	defer unix.Close(dataFD)

	parentFD := dataFD
	for _, component := range components[:len(components)-1] {
		nextFD, openErr := unix.Openat(
			parentFD,
			component,
			unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC|unix.O_NOFOLLOW,
			0,
		)
		if parentFD != dataFD {
			_ = unix.Close(parentFD)
		}
		if openErr != nil {
			return fmt.Errorf("open SQLite parent directory %q without following links: %w", component, openErr)
		}
		parentFD = nextFD
	}
	if parentFD != dataFD {
		defer unix.Close(parentFD)
	}

	if err = requireRuntimeWritableDirectory(parentFD, uid, gid); err != nil {
		return fmt.Errorf("SQLite parent directory is not prepared for the runtime user: %w", err)
	}

	databaseName := components[len(components)-1]
	entries, err := readDirectoryEntries(parentFD)
	if err != nil {
		return fmt.Errorf("list SQLite runtime directory: %w", err)
	}
	entryNames := make(map[string]fs.DirEntry, len(entries))
	for _, entry := range entries {
		entryNames[entry.Name()] = entry
	}

	mainEntry := entryNames[databaseName]
	if mainEntry == nil {
		if entryNames[databaseName+"-wal"] != nil || entryNames[databaseName+"-shm"] != nil {
			return errors.New("SQLite sidecar exists without the configured main database")
		}
		for name := range entryNames {
			if tumblrCoordinationLockName.MatchString(name) {
				return errors.New("tumblr coordination lock exists without the configured main database")
			}
		}
		return nil
	}

	targets := []string{databaseName}
	if entryNames[databaseName+"-wal"] != nil {
		targets = append(targets, databaseName+"-wal")
	}
	if entryNames[databaseName+"-shm"] != nil {
		targets = append(targets, databaseName+"-shm")
	}
	for name := range entryNames {
		if tumblrCoordinationLockName.MatchString(name) {
			targets = append(targets, name)
		}
	}

	for _, name := range targets {
		if err = repairExistingRuntimeFileAt(parentFD, name, uid, gid); err != nil {
			return fmt.Errorf("repair SQLite runtime file %q: %w", name, err)
		}
	}
	return nil
}

func requireRuntimeWritableDirectory(directoryFD, uid, gid int) error {
	var stat unix.Stat_t
	if err := unix.Fstat(directoryFD, &stat); err != nil {
		return err
	}
	if stat.Mode&unix.S_IFMT != unix.S_IFDIR {
		return errors.New("expected a directory")
	}
	if int(stat.Uid) != uid || int(stat.Gid) != gid {
		return fmt.Errorf("ownership is %d:%d, expected %d:%d", stat.Uid, stat.Gid, uid, gid)
	}
	if stat.Mode&0o700 != 0o700 {
		return fmt.Errorf("owner permissions are %04o, expected read/write/search", stat.Mode&0o7777)
	}
	return nil
}

func readDirectoryEntries(directoryFD int) ([]fs.DirEntry, error) {
	duplicateFD, err := unix.Dup(directoryFD)
	if err != nil {
		return nil, err
	}
	directory := os.NewFile(uintptr(duplicateFD), "sqlite-runtime-directory")
	if directory == nil {
		_ = unix.Close(duplicateFD)
		return nil, errors.New("wrap SQLite runtime directory descriptor")
	}
	defer directory.Close()
	return directory.ReadDir(-1)
}

func repairExistingRuntimeFileAt(directoryFD int, name string, uid, gid int) error {
	fileFD, err := unix.Openat(
		directoryFD,
		name,
		unix.O_RDONLY|unix.O_NONBLOCK|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0,
	)
	if err != nil {
		return err
	}
	defer unix.Close(fileFD)

	var opened, pathStat unix.Stat_t
	if err = unix.Fstat(fileFD, &opened); err != nil {
		return err
	}
	if opened.Mode&unix.S_IFMT != unix.S_IFREG {
		return fmt.Errorf("expected a regular file")
	}
	if opened.Nlink != 1 {
		return fmt.Errorf("expected one hard link, found %d", opened.Nlink)
	}
	if err = unix.Fstatat(directoryFD, name, &pathStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if !sameUnixFile(opened, pathStat) {
		return errors.New("path changed while validating it")
	}
	if err = unix.Fchown(fileFD, uid, gid); err != nil {
		return err
	}
	if err = unix.Fchmod(fileFD, 0o600); err != nil {
		return err
	}
	if err = unix.Fstat(fileFD, &opened); err != nil {
		return err
	}
	if int(opened.Uid) != uid || int(opened.Gid) != gid {
		return fmt.Errorf("ownership is %d:%d after repair, expected %d:%d", opened.Uid, opened.Gid, uid, gid)
	}
	if opened.Mode&0o7777 != 0o600 {
		return fmt.Errorf("permissions are %04o after repair, expected 0600", opened.Mode&0o7777)
	}
	if opened.Nlink != 1 {
		return fmt.Errorf("hard-link count changed while repairing file")
	}
	if err = unix.Fstatat(directoryFD, name, &pathStat, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		return err
	}
	if !sameUnixFile(opened, pathStat) {
		return errors.New("path changed while repairing it")
	}
	return nil
}

func sameUnixFile(first, second unix.Stat_t) bool {
	return first.Dev == second.Dev && first.Ino == second.Ino
}
