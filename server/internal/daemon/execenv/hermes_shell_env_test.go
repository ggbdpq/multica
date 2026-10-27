package execenv

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// The task-scoped MULTICA_* environment the daemon assembles for a Hermes run.
var hermesTaskEnv = map[string]string{
	"MULTICA_TOKEN":                "mat_task",
	"MULTICA_TASK_ID":              "task-1",
	"MULTICA_TASK_CONFIG_ROOT":     "/task/multica-config",
	"MULTICA_TASK_WORKSPACES_ROOT": "/workspaces",
	"MULTICA_AGENT_ID":             "agent-1",
	"MULTICA_AGENT_NAME":           "Hermes Agent",
	"MULTICA_WORKSPACE_ID":         "ws-1",
	"MULTICA_SERVER_URL":           "https://task.example",
	"MULTICA_DAEMON_PORT":          "8080",
	"MULTICA_TASK_SLOT":            "3",
	"HOME":                         "/home/daemon-user",
	"PATH":                         "/usr/bin:/bin",
	"HERMES_HOME":                  "/task/env/hermes-home",
	"ANTHROPIC_API_KEY":            "agent-custom-secret",
}

// configEnvPassthrough reads terminal.env_passthrough out of a config.yaml.
func configEnvPassthrough(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var parsed struct {
		Terminal struct {
			EnvPassthrough []string `yaml:"env_passthrough"`
		} `yaml:"terminal"`
	}
	if err := yaml.Unmarshal(data, &parsed); err != nil {
		t.Fatalf("parse %s: %v\n%s", path, err, data)
	}
	return parsed.Terminal.EnvPassthrough
}

func TestHermesShellEnvPassthroughReturnsExactTaskNames(t *testing.T) {
	t.Parallel()

	names := HermesShellEnvPassthrough(hermesTaskEnv)
	if len(names) == 0 {
		t.Fatalf("HermesShellEnvPassthrough returned no names for the task env")
	}
	for _, name := range names {
		if !strings.HasPrefix(name, "MULTICA_") {
			t.Errorf("passthrough includes non-task name %q: %v", name, names)
		}
		if strings.Contains(name, "mat_task") || strings.ContainsAny(name, "/") {
			t.Errorf("passthrough entry %q is not a bare variable name", name)
		}
	}
	for _, want := range []string{"MULTICA_TOKEN", "MULTICA_TASK_ID", "MULTICA_SERVER_URL", "MULTICA_TASK_SLOT"} {
		if !slices.Contains(names, want) {
			t.Errorf("passthrough missing %q, got %v", want, names)
		}
	}
	for _, unwanted := range []string{"HOME", "PATH", "HERMES_HOME", "ANTHROPIC_API_KEY"} {
		if slices.Contains(names, unwanted) {
			t.Errorf("passthrough must not include %q, got %v", unwanted, names)
		}
	}
	if !slices.IsSorted(names) {
		t.Errorf("passthrough not sorted: %v", names)
	}

	if got := HermesShellEnvPassthrough(map[string]string{"HOME": "/h"}); len(got) != 0 {
		t.Errorf("env without MULTICA_* yielded %v, want none", got)
	}
}

func TestEnsureHermesShellEnvPassthroughMergesIntoDerivedConfig(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	// Shape of the derived overlay config: user settings preserved, plus the
	// daemon-managed external_dirs and disabled memory provider.
	derived := "model:\n  provider: anthropic\nskills:\n  external_dirs:\n    - /shared/.hermes/skills\nmemory:\n  provider: \"\"\nterminal:\n  env_passthrough:\n    - USER_TOOL_TOKEN\n"
	if err := os.WriteFile(configPath, []byte(derived), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	names := HermesShellEnvPassthrough(hermesTaskEnv)
	if err := EnsureHermesShellEnvPassthrough(configPath, names, testLogger()); err != nil {
		t.Fatalf("EnsureHermesShellEnvPassthrough: %v", err)
	}

	got := configEnvPassthrough(t, configPath)
	if !slices.Contains(got, "USER_TOOL_TOKEN") {
		t.Errorf("operator-configured entry USER_TOOL_TOKEN was dropped: %v", got)
	}
	for _, want := range []string{"MULTICA_TOKEN", "MULTICA_TASK_CONFIG_ROOT", "MULTICA_TASK_WORKSPACES_ROOT", "MULTICA_TASK_SLOT"} {
		if !slices.Contains(got, want) {
			t.Errorf("terminal.env_passthrough missing %q, got %v", want, got)
		}
	}
	// User entries keep their position ahead of the appended task names.
	if got[0] != "USER_TOOL_TOKEN" {
		t.Errorf("operator entry no longer first: %v", got)
	}
	// Other config content survives untouched.
	data, _ := os.ReadFile(configPath)
	if !strings.Contains(string(data), "provider: anthropic") || !strings.Contains(string(data), "external_dirs") {
		t.Errorf("derived config lost unrelated content:\n%s", data)
	}
	// Values must never reach the config file.
	if strings.Contains(string(data), "mat_task") {
		t.Errorf("config.yaml contains the token value:\n%s", data)
	}
}

func TestEnsureHermesShellEnvPassthroughCreatesTerminalSection(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("skills:\n  external_dirs:\n    - /shared/.hermes/skills\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	names := HermesShellEnvPassthrough(hermesTaskEnv)
	if err := EnsureHermesShellEnvPassthrough(configPath, names, testLogger()); err != nil {
		t.Fatalf("EnsureHermesShellEnvPassthrough: %v", err)
	}

	got := configEnvPassthrough(t, configPath)
	if len(got) != len(names) || !slices.Equal(got, names) {
		t.Errorf("terminal.env_passthrough = %v, want %v", got, names)
	}
}

func TestEnsureHermesShellEnvPassthroughIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(configPath, []byte("model:\n  provider: anthropic\n"), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	names := HermesShellEnvPassthrough(hermesTaskEnv)
	if err := EnsureHermesShellEnvPassthrough(configPath, names, testLogger()); err != nil {
		t.Fatalf("first EnsureHermesShellEnvPassthrough: %v", err)
	}
	afterFirst, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}

	if err := EnsureHermesShellEnvPassthrough(configPath, names, testLogger()); err != nil {
		t.Fatalf("second EnsureHermesShellEnvPassthrough: %v", err)
	}
	afterSecond, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(afterFirst) != string(afterSecond) {
		t.Errorf("second call rewrote the config:\nfirst:\n%s\nsecond:\n%s", afterFirst, afterSecond)
	}
}

func TestEnsureHermesShellEnvPassthroughLeavesUnparseableConfigUntouched(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "config.yaml")
	// The derivation copies an unparseable shared config verbatim (hermes_home.go)
	// so auth/model settings survive; the passthrough merge must keep that
	// tolerance instead of failing the task.
	unparseable := "model:\n  provider: [anthropic\n\tbad:\n  - :\n"
	if err := os.WriteFile(configPath, []byte(unparseable), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}

	if err := EnsureHermesShellEnvPassthrough(configPath, []string{"MULTICA_TOKEN"}, testLogger()); err != nil {
		t.Fatalf("unparseable config must not fail the task, got: %v", err)
	}
	data, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatalf("read config: %v", err)
	}
	if string(data) != unparseable {
		t.Errorf("unparseable config was modified:\n%s", data)
	}
}

func TestEnsureHermesShellEnvPassthroughFailsOnMissingConfig(t *testing.T) {
	t.Parallel()

	// The derivation always writes config.yaml, so a missing file means the
	// overlay itself is broken — fail closed instead of silently launching
	// without the passthrough.
	if err := EnsureHermesShellEnvPassthrough(filepath.Join(t.TempDir(), "config.yaml"), []string{"MULTICA_TOKEN"}, testLogger()); err == nil {
		t.Fatalf("missing config.yaml must be an error")
	}
}
