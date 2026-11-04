package execenv

import (
	"os"
	"path/filepath"
	"testing"
)

func TestIssueSessionRetainsNestedWorktreeAndEnvironmentOnReuse(t *testing.T) {
	repo := newTestRepo(t)
	if err := os.MkdirAll(filepath.Join(repo, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeFile(t, filepath.Join(repo, "subdir", "source.txt"), "source\n")
	gitRun(t, repo, "add", "subdir")
	gitRun(t, repo, "commit", "-m", "nested source")
	root := t.TempDir()
	task := TaskContextForEnv{IssueID: testBranchOwner.ConversationID, AgentID: testBranchOwner.AgentID, IssueAgentSessionID: "persistent-session", AgentName: "J"}
	env, err := Prepare(PrepareParams{WorkspacesRoot: root, WorkspaceID: testBranchOwner.WorkspaceID, TaskID: "11112222-3333-4444-5555-666677778888", Provider: "claude", AgentName: "J", LocalWorktree: &LocalWorktreeParams{LocalPath: filepath.Join(repo, "subdir")}, Task: task}, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer env.ReleaseLock()
	writeFile(t, filepath.Join(env.WorkDir, "unfinished.txt"), "preserve me\n")
	provenance, err := ReadManagedEnvProvenance(env.RootDir)
	if err != nil || provenance.IssueAgentSessionID != task.IssueAgentSessionID || provenance.WorkDir != env.WorkDir {
		t.Fatalf("session provenance=%+v err=%v", provenance, err)
	}
	resumed := Reuse(ReuseParams{WorkspacesRoot: root, EnvRoot: env.RootDir, WorkDir: env.WorkDir, Provider: "claude", Task: task}, testLogger())
	if resumed == nil {
		t.Fatal("persistent nested worktree was not reusable")
	}
	if resumed.RootDir != env.RootDir || resumed.WorkDir != env.WorkDir {
		t.Fatalf("resume moved environment: old=%s new=%s", env.RootDir, resumed.RootDir)
	}
	if got := readFile(t, filepath.Join(resumed.WorkDir, "unfinished.txt")); got != "preserve me\n" {
		t.Fatalf("work lost: %q", got)
	}
	if _, err := os.Stat(filepath.Join(repo, "subdir", "unfinished.txt")); !os.IsNotExist(err) {
		t.Fatalf("session wrote into user checkout: %v", err)
	}
}
