package e2e

import (
	"fmt"
	"net/http"
	"path"
	"strconv"
	"testing"
	"time"
)

var defaultPurgeConfig = purgeProcessConfig{
	execPath: "../bin/etcd-purge",
}

func TestPurge(t *testing.T) {
	size := 3

	cfg := defaultConfig
	cfg.execPath = etcdExecLatest
	cfg.clusterSize = size

	etcdClus1, err := cfg.NewEtcdProcessCluster()
	if err != nil {
		t.Fatal(err)
	}
	if err = etcdClus1.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = etcdClus1.Stop(10 * time.Second); err != nil {
			t.Fatal(err)
		}
	}()

	// set up discovery server for each node
	procs := make([]*discoveryProcess, size)
	errc := make(chan error)
	for i := 0; i < size; i++ {
		dcfg := discoveryProcessConfig{
			execPath: discoveryExec,
			etcdEp:   etcdClus1.procs[i].cfg.curl.String(),
		}
		procs[i] = dcfg.NewDiscoveryProcess()
		go func(dp *discoveryProcess) {
			errc <- dp.Start()
		}(procs[i])
	}
	for i := 0; i < size; i++ {
		if err = <-errc; err != nil {
			t.Fatal(err)
		}
	}

	// create a discovery token for a new etcd cluster
	etcdClusterSize := 5
	req := cURLReq{
		timeout:  5 * time.Second,
		endpoint: fmt.Sprintf("http://localhost:%d/new?size=%d", procs[0].cfg.webPort, etcdClusterSize),
		method:   http.MethodGet,
		expFunc:  tokenFunc,
	}
	var token string
	token, err = req.Send()
	if err != nil {
		t.Fatal(err)
	}
	if !tokenFunc(token) {
		t.Fatalf("unexpected token %q", token)
	}

	// start etcd on top of discovery
	etcdCfg := defaultConfig
	etcdCfg.execPath = etcdExecLatest
	etcdCfg.clusterSize = etcdClusterSize
	etcdCfg.discoveryToken = token
	etcdClus2, err := etcdCfg.NewEtcdProcessCluster()
	if err != nil {
		t.Fatal(err)
	}
	if err = etcdClus2.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = etcdClus2.Stop(10 * time.Second); err != nil {
			t.Fatal(err)
		}
	}()

	// create more tokens
	tokens := []string{token}
	for i := 0; i < 10; i++ {
		var tk string
		tk, err = req.Send()
		if err != nil {
			t.Fatal(err)
		}
		if !tokenFunc(tk) {
			t.Fatalf("unexpected token %q", tk)
		}
		tokens = append(tokens, tk)

		time.Sleep(100 * time.Millisecond)
	}

	// start purge tool
	purgeCfg := defaultPurgeConfig
	purgeCfg.etcdEps = etcdClus1.ClientEndpoints()
	purgeCfg.dir = path.Join("_etcd", "registry")
	purgeCfg.tsDir = "purge_ts"
	purgeCfg.dry = false
	purgeCfg.purgeInterval = time.Second
	purgeCfg.keepInterval = 5 * time.Second
	purgeCfg.oldV2 = true
	purgeProc := purgeCfg.NewPurgeProcess()
	if err = purgeProc.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err = purgeProc.Stop(10 * time.Second); err != nil {
			t.Fatal(err)
		}
	}()

	time.Sleep(purgeCfg.keepInterval + 3*time.Second)

	// ensure that purge tool remove all tokens
	for _, tk := range tokens {
		// validate token writes on etcd starts
		ctlv2Discovery := etcdctlCtl{
			api:         2,
			execPath:    etcdctlExecLatest,
			endpoints:   etcdClus1.ClientEndpoints(),
			dialTimeout: 7 * time.Second,
		}
		if err = ctlv2Discovery.Get("l", KV{
			Key: path.Join("_etcd", "registry", path.Base(tk), "_config", "size"),
			Val: fmt.Sprintf("Error:  100: Key not found (/_etcd/registry/%s)", path.Base(tk)),
		}); err != nil {
			t.Fatal(err)
		}
	}

	// create more tokens
	newTokens := []string{}
	for i := 0; i < 5; i++ {
		var tk string
		tk, err = req.Send()
		if err != nil {
			t.Fatal(err)
		}
		if !tokenFunc(tk) {
			t.Fatalf("unexpected token %q", tk)
		}
		newTokens = append(newTokens, tk)

		time.Sleep(100 * time.Millisecond)
	}

	time.Sleep(time.Second)

	// check before 'keepInterval'
	for _, tk := range newTokens {
		// validate token writes on etcd starts
		ctlv2Discovery := etcdctlCtl{
			api:         2,
			execPath:    etcdctlExecLatest,
			endpoints:   etcdClus1.ClientEndpoints(),
			dialTimeout: 7 * time.Second,
		}
		if err = ctlv2Discovery.Get("l", KV{
			Key: path.Join("_etcd", "registry", path.Base(tk), "_config", "size"),
			Val: strconv.Itoa(etcdClusterSize),
		}); err != nil {
			t.Fatal(err)
		}
	}
}
