package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const repairSQLiteOwnershipCommand = "repair-sqlite-ownership"

var supportedSQLiteOwnershipDrivers = map[string]struct{}{
	"litestream":              {},
	"sqlite3-fk-wal":          {},
	"sqlite3-fk-wal-fullsync": {},
}

func runSQLiteOwnershipRepairCLI(args []string) (handled bool, exitCode int) {
	if len(args) == 0 || args[0] != repairSQLiteOwnershipCommand {
		return false, 0
	}

	flags := flag.NewFlagSet(repairSQLiteOwnershipCommand, flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", "", "path to the bridge config")
	dataDir := flags.String("data-dir", "", "bridge runtime data directory")
	uidText := flags.String("uid", "", "numeric runtime user ID")
	gidText := flags.String("gid", "", "numeric runtime group ID")
	if err := flags.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return true, 0
		}
		return true, 2
	}
	if flags.NArg() != 0 {
		_, _ = fmt.Fprintln(os.Stderr, "repair-sqlite-ownership does not accept positional arguments")
		return true, 2
	}

	uid, err := parseRuntimeOwnershipID("uid", *uidText)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return true, 2
	}
	gid, err := parseRuntimeOwnershipID("gid", *gidText)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		return true, 2
	}
	resolvedDataDir, err := resolveOwnershipDataDir(*dataDir)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Refusing to repair SQLite ownership: %v\n", err)
		return true, 1
	}
	resolvedConfigPath, err := resolveOwnershipConfigPath(resolvedDataDir, *configPath)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Refusing to repair SQLite ownership: %v\n", err)
		return true, 1
	}

	// LoadConfig applies the same config upgrades and env_config_prefix overrides
	// as normal startup, but SaveConfig=false keeps this privileged preflight
	// read-only. It does not initialize logging or open the database.
	bridgeMain.ConfigPath = resolvedConfigPath
	bridgeMain.SaveConfig = false
	bridgeMain.LoadConfig()

	databasePath, isFile, err := sqliteOwnershipDatabasePath(
		bridgeMain.Config.Database.Type,
		bridgeMain.Config.Database.URI,
	)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Refusing to repair SQLite ownership: %v\n", err)
		return true, 1
	}
	if !isFile {
		return true, 0
	}
	if err = repairSQLiteOwnership(resolvedDataDir, databasePath, uid, gid); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Refusing to repair SQLite ownership: %v\n", err)
		return true, 1
	}
	return true, 0
}

func parseRuntimeOwnershipID(name, value string) (int, error) {
	if value == "" {
		return 0, fmt.Errorf("--%s is required", name)
	}
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 || parsed == math.MaxUint32 {
		return 0, fmt.Errorf("--%s must be a non-zero numeric ID", name)
	}
	if strconv.IntSize == 32 && parsed > math.MaxInt32 {
		return 0, fmt.Errorf("--%s is too large on this platform", name)
	}
	return int(parsed), nil
}

func resolveOwnershipDataDir(path string) (string, error) {
	if path == "" {
		return "", errors.New("--data-dir is required")
	}
	if !filepath.IsAbs(path) {
		return "", errors.New("data directory must be absolute")
	}
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect data directory: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return "", errors.New("data directory must be a real directory, not a symbolic link")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("canonicalize data directory: %w", err)
	}
	return filepath.Clean(resolved), nil
}

func resolveOwnershipConfigPath(dataDir, path string) (string, error) {
	if path == "" {
		return "", errors.New("--config is required")
	}
	if !filepath.IsAbs(path) {
		path = filepath.Join(dataDir, path)
	}
	path = filepath.Clean(path)
	info, err := os.Lstat(path)
	if err != nil {
		return "", fmt.Errorf("inspect config: %w", err)
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return "", errors.New("config must be a regular, non-symbolic-link file")
	}
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		return "", fmt.Errorf("canonicalize config: %w", err)
	}
	if !pathInsideDirectory(dataDir, resolved) {
		return "", errors.New("config must be inside the data directory")
	}
	return filepath.Clean(resolved), nil
}

func sqliteOwnershipDatabasePath(databaseType, databaseURI string) (string, bool, error) {
	if strings.HasPrefix(strings.ToLower(databaseType), "postgres") || databaseType == "pgx" {
		return "", false, nil
	}
	if _, ok := supportedSQLiteOwnershipDrivers[databaseType]; !ok {
		return "", false, fmt.Errorf("database type %q is not a supported SQLite runtime driver", databaseType)
	}
	if databaseURI == "" || databaseURI == ":memory:" {
		return "", false, nil
	}
	if len(databaseURI) >= len("file:") && strings.EqualFold(databaseURI[:len("file:")], "file:") &&
		!strings.HasPrefix(databaseURI, "file:") {
		return "", false, errors.New("SQLite file URI prefix must use lowercase file")
	}

	if strings.HasPrefix(databaseURI, "file:") {
		parsed, err := url.Parse(databaseURI)
		if err != nil {
			return "", false, fmt.Errorf("parse SQLite file URI: %w", err)
		}
		if parsed.User != nil {
			return "", false, errors.New("SQLite file URI must not contain user information")
		}
		if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
			return "", false, fmt.Errorf("SQLite file URI host %q is not local", parsed.Host)
		}
		if parsed.Fragment != "" {
			return "", false, errors.New("SQLite file URI fragments are not supported")
		}
		query, err := url.ParseQuery(parsed.RawQuery)
		if err != nil {
			return "", false, fmt.Errorf("parse SQLite file URI options: %w", err)
		}
		if query.Get("vfs") != "" {
			return "", false, errors.New("SQLite custom VFS is not supported by ownership repair")
		}
		if strings.EqualFold(query.Get("mode"), "memory") {
			return "", false, nil
		}

		var path string
		if parsed.Opaque != "" {
			path, err = url.PathUnescape(parsed.Opaque)
			if err != nil {
				return "", false, fmt.Errorf("decode SQLite file path: %w", err)
			}
		} else {
			path = parsed.Path
		}
		if path == "" || path == ":memory:" {
			return "", false, nil
		}
		if strings.IndexByte(path, 0) >= 0 {
			return "", false, errors.New("SQLite file path contains a NUL byte")
		}
		return path, true, nil
	}

	path, rawQuery, _ := strings.Cut(databaseURI, "?")
	query, err := url.ParseQuery(rawQuery)
	if err != nil {
		return "", false, fmt.Errorf("parse SQLite options: %w", err)
	}
	if query.Get("vfs") != "" {
		return "", false, errors.New("SQLite custom VFS is not supported by ownership repair")
	}
	if path == "" || path == ":memory:" {
		return "", false, nil
	}
	if strings.IndexByte(path, 0) >= 0 {
		return "", false, errors.New("SQLite file path contains a NUL byte")
	}
	return path, true, nil
}

func pathInsideDirectory(directory, path string) bool {
	relative, err := filepath.Rel(directory, path)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator)) && !filepath.IsAbs(relative)
}
