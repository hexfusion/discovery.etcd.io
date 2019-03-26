package e2e

import (
	"fmt"
	"strings"
	"time"

	"github.com/coreos/etcd/pkg/expect"
)

type purgeProcessConfig struct {
	execPath      string
	etcdEps       []string
	dir           string
	tsDir         string
	dry           bool
	purgeInterval time.Duration
	keepInterval  time.Duration
	oldV2         bool
}

type purgeProcess struct {
	cfg   *purgeProcessConfig
	proc  *expect.ExpectProcess
	donec chan struct{} // closed when Interact() terminates
}

// NewPurgeProcess creates a new 'purgeProcess'.
func (cfg *purgeProcessConfig) NewPurgeProcess() *purgeProcess {
	copied := *cfg
	return &purgeProcess{cfg: &copied}
}

func (pp *purgeProcess) Stop(d time.Duration) (err error) {
	errc := make(chan error, 1)
	go func() { errc <- pp.proc.Stop() }()
	select {
	case err := <-errc:
		return err
	case <-time.After(d):
		return fmt.Errorf("took longer than %v to Stop cluster", d)
	}
}

func (pp *purgeProcess) Start() error {
	args := []string{
		pp.cfg.execPath,
		"-logtostderr",
		fmt.Sprintf("-endpoints=%s", strings.Join(pp.cfg.etcdEps, ",")),
		fmt.Sprintf("-dir=%s", pp.cfg.dir),
		fmt.Sprintf("-timestamp-dir=%s", pp.cfg.tsDir),
		fmt.Sprintf("-dry=%v", pp.cfg.dry),
		fmt.Sprintf("-purge-interval=%s", pp.cfg.purgeInterval),
		fmt.Sprintf("-keep-interval=%s", pp.cfg.keepInterval),
		fmt.Sprintf("-old-v2cluster=%v", pp.cfg.oldV2),
	}
	fmt.Println(args)
	child, err := expect.NewExpect(args[0], args[1:]...)
	if err != nil {
		return err
	}
	pp.proc = child
	pp.donec = make(chan struct{})

	readyC := make(chan error)
	go func() {
		readyC <- pp.waitReady()
	}()
	select {
	case err = <-readyC:
		if err != nil {
			return err
		}
	case <-time.After(10 * time.Second):
		return fmt.Errorf("timed out waiting for discover server")
	}
	return nil
}

func (pp *purgeProcess) waitReady() error {
	defer close(pp.donec)
	return waitReadyExpectProcPurge(pp.proc)
}

func waitReadyExpectProcPurge(exproc *expect.ExpectProcess) error {
	readyStrs := []string{"starting with dir", "writing timestamp"}
	c := 0
	matchSet := func(l string) bool {
		for _, s := range readyStrs {
			if strings.Contains(l, s) {
				c++
				break
			}
		}
		return c == len(readyStrs)
	}
	_, err := exproc.ExpectFunc(matchSet)
	return err
}
