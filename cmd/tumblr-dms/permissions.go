package main

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	"maunium.net/go/mautrix/bridgev2/matrix/mxmain"
)

type sensitivePath struct {
	description string
	path        string
}

func secureRuntimeFiles(br *mxmain.BridgeMain) error {
	if br == nil || br.Config == nil {
		return fmt.Errorf("bridge configuration is unavailable")
	}

	cwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("find the runtime directory: %w", err)
	}
	cwd = filepath.Clean(cwd)

	paths := []sensitivePath{
		{description: "config", path: br.ConfigPath},
		{description: "registration", path: br.RegistrationPath},
		{description: "runtime log", path: filepath.Join(cwd, "bridge.log")},
	}

	databasePath, isFile, err := sqliteDatabasePath(br.Config.Database.Type, br.Config.Database.URI)
	if err != nil {
		return fmt.Errorf("identify the configured SQLite database: %w", err)
	}
	if isFile {
		paths = append(paths,
			sensitivePath{description: "database", path: databasePath},
			sensitivePath{description: "database WAL", path: databasePath + "-wal"},
			sensitivePath{description: "database shared memory", path: databasePath + "-shm"},
		)
	}
	readOnlyDatabase := br.Config.Database.ReadOnlyPool
	if readOnlyDatabase.MaxOpenConns > 0 && readOnlyDatabase.URI != "" {
		readOnlyType := readOnlyDatabase.Type
		if readOnlyType == "" {
			readOnlyType = br.Config.Database.Type
		}
		readOnlyPath, readOnlyIsFile, readOnlyErr := sqliteDatabasePath(readOnlyType, readOnlyDatabase.URI)
		if readOnlyErr != nil {
			return fmt.Errorf("identify the configured read-only SQLite database: %w", readOnlyErr)
		}
		if readOnlyIsFile {
			paths = append(paths,
				sensitivePath{description: "read-only database", path: readOnlyPath},
				sensitivePath{description: "read-only database WAL", path: readOnlyPath + "-wal"},
				sensitivePath{description: "read-only database shared memory", path: readOnlyPath + "-shm"},
			)
		}
	}

	for _, writer := range br.Config.Logging.Writers {
		if string(writer.Type) == "file" && writer.Filename != "" {
			paths = append(paths, sensitivePath{description: "configured log", path: writer.Filename})
		}
	}

	seen := make(map[string]struct{}, len(paths))
	for _, candidate := range paths {
		if candidate.path == "" {
			continue
		}
		path := absolutePath(cwd, candidate.path)
		if _, duplicate := seen[path]; duplicate {
			continue
		}
		seen[path] = struct{}{}
		if err = secureExistingFile(path, 0o600); err != nil {
			return fmt.Errorf("%s %q: %w", candidate.description, path, err)
		}
	}
	return nil
}

func absolutePath(cwd, path string) string {
	if !filepath.IsAbs(path) {
		path = filepath.Join(cwd, path)
	}
	return filepath.Clean(path)
}

func sqliteDatabasePath(databaseType, databaseURI string) (string, bool, error) {
	databaseType = strings.ToLower(strings.TrimSpace(databaseType))
	if !strings.HasPrefix(databaseType, "sqlite") && !strings.HasPrefix(databaseType, "litestream") {
		return "", false, nil
	}

	if databaseURI == "" || databaseURI == ":memory:" {
		return "", false, nil
	}

	if strings.HasPrefix(databaseURI, "file:") {
		parsed, err := url.Parse(databaseURI)
		if err != nil {
			return "", false, err
		}
		if parsed.Host != "" && !strings.EqualFold(parsed.Host, "localhost") {
			return "", false, fmt.Errorf("unsupported SQLite file URI host %q", parsed.Host)
		}
		query, err := url.ParseQuery(parsed.RawQuery)
		if err != nil {
			return "", false, fmt.Errorf("parse SQLite URI options: %w", err)
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
		return path, true, nil
	}

	path, _, _ := strings.Cut(databaseURI, "?")
	if path == "" || path == ":memory:" {
		return "", false, nil
	}
	return path, true, nil
}
