package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

// openSpoolFiles lists this process's open anonymous spools under dir: an
// O_TMPFILE fd links to "<spool dir>/#<inode> (deleted)".
func openSpoolFiles(dir string) ([]string, error) {
	prefix := filepath.Join(dir, spoolDirName) + string(os.PathSeparator)
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name())); err == nil && strings.HasPrefix(target, prefix) {
			out = append(out, target)
		}
	}
	return out, nil
}

// D3: every spool is anonymous, 0600, close-on-exec, and never named.
func TestSpoolIsAnonymousCloexec0600(t *testing.T) {
	dir := t.TempDir()
	k := &keeper{cfg: config{storeDir: dir}, work: newWorkingSet()}
	f, err := k.newSpool()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("private")); err != nil {
		t.Fatal(err)
	}
	var st unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &st); err != nil {
		t.Fatal(err)
	}
	if st.Nlink != 0 || st.Mode&0777 != 0600 {
		t.Fatalf("spool nlink=%d mode=%o; want anonymous 0600", st.Nlink, st.Mode&0777)
	}
	flags, err := unix.FcntlInt(f.Fd(), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		t.Fatalf("spool fd not close-on-exec: flags=%d err=%v", flags, err)
	}
	ns := filepath.Join(dir, spoolDirName)
	if info, err := os.Stat(ns); err != nil || info.Mode().Perm() != 0700 {
		t.Fatalf("spool dir mode: %v %v", info, err)
	}
	if names, _ := os.ReadDir(ns); len(names) != 0 {
		t.Fatalf("spool dir holds names: %v", names)
	}
	if open, _ := openSpoolFiles(dir); len(open) != 1 {
		t.Fatalf("open spools=%v; want 1", open)
	}
	disk := k.work.disk
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if open, _ := openSpoolFiles(dir); len(open) != 0 || k.work.disk != disk-7 {
		t.Fatalf("close left open=%v disk=%d", open, k.work.disk)
	}
	if err := checkSpools(dir); err != nil {
		t.Fatalf("clean spool dir refused: %v", err)
	}
}

// D3: a regular file in the spool dir is a startup refusal naming it; the
// file is left untouched (no sweep).
func TestStartupRefusesRegularFileInSpoolDir(t *testing.T) {
	dir := t.TempDir()
	if err := checkSpools(dir); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(dir, spoolDirName, "turn-stale")
	if err := os.WriteFile(name, []byte("stale"), 0600); err != nil {
		t.Fatal(err)
	}
	err := checkSpools(dir)
	if err == nil || !strings.Contains(err.Error(), name) {
		t.Fatalf("regular file accepted: %v", err)
	}
	if data, _ := os.ReadFile(name); string(data) != "stale" {
		t.Fatal("startup check touched the file")
	}
}

func TestSpoolDirSymlinkOrModeRefused(t *testing.T) {
	dir := t.TempDir()
	if err := os.Symlink(t.TempDir(), filepath.Join(dir, spoolDirName)); err != nil {
		t.Fatal(err)
	}
	if err := checkSpools(dir); err == nil {
		t.Fatal("symlinked spool dir accepted")
	}
	k := &keeper{cfg: config{storeDir: dir}, work: newWorkingSet()}
	if _, err := k.newSpool(); err == nil {
		t.Fatal("spool opened through a symlinked dir")
	}
	wide := t.TempDir()
	if err := os.Mkdir(filepath.Join(wide, spoolDirName), 0755); err != nil {
		t.Fatal(err)
	}
	if err := checkSpools(wide); err == nil {
		t.Fatal("0755 spool dir accepted")
	}
}

// D3: no named fallback. A filesystem without O_TMPFILE refuses startup.
func TestSpoolRefusesWithoutTmpfile(t *testing.T) {
	_, err := openAnonymous(filepath.Join(t.TempDir(), "missing"))
	if err == nil || !strings.Contains(err.Error(), "O_TMPFILE") {
		t.Fatalf("no O_TMPFILE refusal: %v", err)
	}
}
