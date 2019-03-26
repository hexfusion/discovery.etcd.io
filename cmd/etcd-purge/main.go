// Reference https://github.com/xiang90/etcd_discovery_purge

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/coreos/etcd/client"
	"github.com/golang/glog"
)

func main() {
	eps := flag.String("endpoints", "http://127.0.0.1:2379", "comma separated endpoints of an etcd cluster")
	dir := flag.String("dir", path.Join("_etcd", "registry"), "first level directory to purge")
	tsDir := flag.String("timestamp-dir", "purge_ts", "directory to store and retrieve fuzzy timestamps")
	dry := flag.Bool("dry", true, "do not actually purge the directories")
	purgeIdx := flag.Uint64("purge-index", 0, "the min index of directory to purge, 0 to purge nothing")
	purgeInterval := flag.Duration("purge-interval", time.Duration(0), "purge interval, 0 to run only once")
	keepInterval := flag.Duration("keep-interval", 7*24*time.Hour, "interval duration to fetch timestamps")
	oldv2 := flag.Bool("old-v2cluster", false, "purge old v2 cluster")
	flag.Parse()

	glog.Infof("starting with dir %q, timestamp dir %q, purge index %d, purge interval %v, keep interval %v",
		*dir, *tsDir, *purgeIdx, *purgeInterval, *keepInterval)

	hc, err := client.New(client.Config{Endpoints: strings.Split(*eps, ",")})
	if err != nil {
		glog.Fatal(err)
		os.Exit(1)
	}
	kapi := client.NewKeysAPI(hc)

	if *purgeInterval > time.Duration(0) {
		go makeTimestamps(kapi, *tsDir, *dry, *purgeInterval)

		glog.Infof("sleeping %v after makeTimestamps", *purgeInterval)
		time.Sleep(*purgeInterval + 2*time.Second)
	}

	for {
		idx := *purgeIdx
		if idx == 0 {
			idx = findPurgeIndex(kapi, *tsDir, *purgeInterval, *keepInterval)
		}

		if idx == 0 {
			glog.Infof("min-index is 0")
			if *purgeInterval == time.Duration(0) {
				return
			}

			glog.Infof("retry after %v", *purgeInterval)
			time.Sleep(*purgeInterval)
			continue
		}

		runPurge(kapi, *dir, idx, *dry, *oldv2)
		if *purgeInterval == time.Duration(0) {
			return
		}

		glog.Infof("sleeping %v", *purgeInterval)
		time.Sleep(*purgeInterval)
	}
}

func makeTimestamps(kapi client.KeysAPI, pfx string, dry bool, purgeInterval time.Duration) {
	for {
		for {
			key := path.Join(pfx, fmt.Sprintf("%016x", time.Now().UnixNano()))
			glog.Infof("writing timestamp %q", key)
			if dry {
				return
			}
			_, err := kapi.Set(context.Background(), key, "ts", &client.SetOptions{})
			if err != nil {
				glog.Error(err)
				time.Sleep(time.Second)
				continue
			}
			glog.Infof("wrote timestamp %q", key)

			break
		}

		glog.Infof("sleeping %v before making another one", purgeInterval)
		time.Sleep(purgeInterval)
	}
}

func findPurgeIndex(kapi client.KeysAPI, pfx string, purgeInterval, keepInterval time.Duration) uint64 {
	now := time.Now().UnixNano()

	var resp *client.Response
	for i := 0; ; i++ {
		var err error
		resp, err = kapi.Get(context.Background(), pfx, &client.GetOptions{Sort: true})
		if err == nil {
			break
		}
		glog.Warningf("retrying after %v (%v)", purgeInterval, err)
		time.Sleep(purgeInterval)
		if i >= 10 {
			glog.Fatalf("failed after 10 retries (%v)", err)
			os.Exit(1)
		}
	}

	// now we traverse all timestamps
	for _, n := range resp.Node.Nodes {
		ct, err := strconv.ParseInt(path.Base(n.Key), 16, 64)
		if err != nil {
			glog.Infof("skipped bad timestamp %s (%v)", n.Key, err)
			continue
		}
		if now-ct > keepInterval.Nanoseconds() {
			glog.Infof("%q was created >%v ago; returning %d", n.Key, keepInterval, n.CreatedIndex)
			return n.CreatedIndex
		}
	}

	glog.Infof("could not find any %s/timestamps created %v ago", pfx, keepInterval)
	return 0
}

func runPurge(kapi client.KeysAPI, pfx string, purgeIdx uint64, dry, oldv2 bool) {
	glog.Infof("purging with index %d", purgeIdx)

	resp, err := kapi.Get(context.Background(), pfx, &client.GetOptions{})
	if err != nil {
		glog.Fatal(err)
		os.Exit(1)
	}

	total := 0
	all := 0

	// now we traverse all {UUID}
	for _, n := range resp.Node.Nodes {
		all++
		// do not overload discovery service
		time.Sleep(10 * time.Millisecond)

		var gresp *client.Response
		for i := 0; ; i++ {
			var err error
			gresp, err = kapi.Get(context.Background(), n.Key, &client.GetOptions{})
			if err == nil {
				break
			}
			if i >= 10 {
				glog.Fatalf("failed after 10 retries (%v)", err)
				os.Exit(1)
			}
		}

		if gresp.Node.CreatedIndex > purgeIdx {
			glog.Infof("skipped young uuid directory %q with index %d", n.Key, gresp.Node.CreatedIndex)
			continue
		}

		// _config is a hidden dir, so we only purge "empty" directory.
		if len(gresp.Node.Nodes) != 0 {
			if !oldv2 {
				glog.Infof("skipped non-empty uuid directory %s", n.Key)
				continue
			}
			for _, n := range gresp.Node.Nodes {
				if n.TTL != 0 {
					glog.Info("skipped 0.4 uuid directory %s", n.Key)
					continue
				}
			}
		}

		glog.Infof("found empty/old uuid directory %q and start to purge...", n.Key)
		for i := 0; ; i++ {
			if dry {
				break
			}

			_, err = kapi.Delete(context.Background(), n.Key, &client.DeleteOptions{Recursive: true})
			if err == nil {
				break
			}

			if eerr, ok := err.(client.Error); ok {
				if eerr.Code == client.ErrorCodeKeyNotFound {
					break
				}
			}

			if i >= 10 {
				glog.Fatalf("failed after 10 retries (%v)", err)
				os.Exit(1)
			}
		}
		glog.Infof("successfully purged uuid directory %q", n.Key)

		total++
		if total%10 == 0 {
			glog.Infof("purging %d directories out of %d nodes", total, all)
		}
	}

	glog.Infof("purged with index %d", purgeIdx)
}
