package execenv

import (
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Hermes scrubs its tool subprocess shells (terminal, execute_code, background
// spawns): child environments are filtered against a provider-credential
// blocklist, and a name is released back into those children only when it is
// registered for passthrough — by a skill, or by `terminal.env_passthrough` in
// config.yaml (upstream tools/env_passthrough.py and
// tools/environments/local_env_policy.py). Task-scoped MULTICA_* variables
// injected into the Hermes main process can therefore be missing from
// `multica issue ...` calls made inside a tool shell (issue #9587) — the same
// failure #9438 fixed for Codex with a managed shell_environment_policy.
//
// The daemon owns the remedy through the task-local HERMES_HOME overlay: the
// derived config.yaml is daemon-written (hermes_home.go), so the daemon appends
// the task's MULTICA_* names to terminal.env_passthrough there. The config
// stores NAMES only — values resolve from the child process environment the
// daemon assembled for this task at Hermes' own spawn time. The MULTICA_*
// namespace cannot come from agent custom_env (daemon.go isBlockedEnvKey blocks
// the whole prefix), so every MULTICA_* name in the child env is
// daemon-constructed task context — the same safety argument as the Codex
// allowlist. Non-Multica Hermes sessions never see this config: it lives only
// in a task overlay.

// HermesShellEnvPassthrough returns the sorted, de-duplicated MULTICA_* names
// present in the assembled Hermes child environment. Names only: values never
// enter the config file, and Hermes forwards only variables that exist.
func HermesShellEnvPassthrough(childEnv map[string]string) []string {
	seen := make(map[string]struct{}, len(childEnv))
	for key := range childEnv {
		if strings.HasPrefix(strings.ToUpper(key), "MULTICA_") {
			seen[key] = struct{}{}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// EnsureHermesShellEnvPassthrough merges names into terminal.env_passthrough in
// the task-local derived config.yaml, preserving any operator-configured
// entries the derivation carried over from the shared home. Existing entries
// keep their order; added names are appended sorted. The file is left
// untouched when it already lists every name, so unchanged overlays are not
// rewritten.
//
// Failure semantics mirror the derivation's own tolerance (hermes_home.go
// writeDerivedHermesConfig): a config that does not parse is left
// byte-for-byte untouched with a warning — the derivation already copied it
// verbatim to keep auth/model settings usable, and it cannot be amended
// without a parse — while a read error is fatal because the overlay itself is
// broken (the derivation always writes config.yaml).
func EnsureHermesShellEnvPassthrough(configPath string, names []string, logger *slog.Logger) error {
	if len(names) == 0 {
		return nil
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		return fmt.Errorf("read hermes config.yaml: %w", err)
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		if logger != nil {
			logger.Warn("execenv: hermes-home config parse failed; leaving terminal.env_passthrough unmanaged",
				"config_path", configPath, "error", err)
		}
		return nil
	}
	top := yamlDocumentRoot(&doc)
	if top == nil {
		if logger != nil {
			logger.Warn("execenv: hermes-home config is not a mapping; leaving terminal.env_passthrough unmanaged",
				"config_path", configPath)
		}
		return nil
	}

	terminal := yamlMapValue(top, "terminal")
	if terminal == nil || terminal.Kind != yaml.MappingNode {
		terminal = &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
		yamlSetMapValue(top, "terminal", terminal)
	}
	existing, dropped := hermesPassthroughEntries(terminal)
	merged := make([]string, 0, len(existing)+len(names))
	merged = append(merged, existing...)
	present := make(map[string]struct{}, len(existing))
	for _, name := range existing {
		present[name] = struct{}{}
	}
	for _, name := range names {
		if _, ok := present[name]; ok {
			continue
		}
		present[name] = struct{}{}
		merged = append(merged, name)
	}
	if len(merged) == len(existing) && dropped == 0 {
		return nil
	}
	yamlSetMapValue(terminal, "env_passthrough", yamlStringSeq(merged))
	return marshalYAMLToFile(&doc, configPath)
}

// hermesPassthroughEntries collects the current terminal.env_passthrough scalar
// entries in order, plus how many non-scalar entries were dropped (Hermes
// itself ignores non-string list items). Absent or malformed key yields
// (nil, 0).
func hermesPassthroughEntries(terminal *yaml.Node) (entries []string, dropped int) {
	list := yamlMapValue(terminal, "env_passthrough")
	if list == nil {
		return nil, 0
	}
	// Hermes reads the key as a list; a scalar or mapping is inert there, but
	// replacing it with the managed sequence keeps the file well-formed for the
	// operator channel.
	if list.Kind != yaml.SequenceNode {
		return nil, 1
	}
	for _, item := range list.Content {
		if item.Kind != yaml.ScalarNode {
			dropped++
			continue
		}
		entries = append(entries, item.Value)
	}
	return entries, dropped
}
