package main

import (
	"flag"
	"fmt"
	"github.com/henter/poidb"
	"github.com/rs/xid"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
	"math/rand"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"runtime/debug"
	"sort"
	"syscall"
	"time"
)

var (
	max    int
	dbPath string
	pprof  string
	gogc int
	gcs int
)

type Item struct {
	Id         string
	X, Y, Dist float64
}

func main() {
	flag.IntVar(&max, "max", 1000000, "max poi to test")
	flag.StringVar(&pprof, "pprof", ":9901", "pprof listen")
	flag.StringVar(&dbPath, "path", "/tmp/poi.db", "db file path")
	flag.IntVar(&gogc, "gogc", 20, "go gc")
	flag.IntVar(&gcs, "gcs", 20, "gc second")
	flag.Parse()

	go func() {
		err := http.ListenAndServe(pprof, nil)
		if err != nil {
			fmt.Printf("pprof listen error %s", err)
		}
	}()

	go watchOutOfMemory()

	fmt.Printf("start db %s\n", dbPath)
	//db, err := poidb.Open(":memory:")
	t := time.Now()
	db, err := poidb.Open(dbPath)
	if err != nil {
		fmt.Println("err " + err.Error())
		return
	}

	go func() {
		exit := func() {
			fmt.Printf("db closing.. count %d\n", db.Count())
			err = db.Close()
			if err != nil {
				fmt.Println("db close error " + err.Error())
			}
			os.Exit(0)
		}
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGUSR1)
		for {
			sig := <-ch
			switch sig {
			case syscall.SIGTERM:
				exit()
			case syscall.SIGINT:
				exit()
			case syscall.SIGUSR1:
				exit()
			}
		}
	}()

	fmt.Printf("db loaded %d items in %s\n", db.Count(), time.Since(t))
	err = db.SetConfig(poidb.Config{
		//SyncPolicy:poidb.EverySecond,
		SyncPolicy:           poidb.Never,
		AutoShrinkPercentage: 10000,
	})
	if err != nil {
		fmt.Println("err " + err.Error())
		return
	}

	i := db.Count()
	rand.Seed(time.Now().UnixNano())

	var lng, lat float64
	if i < max {
		for i < max {
			i++
			k := xid.New().String()
			//中国大部分区域经纬度
			lng = 90.123 + float64(rand.Intn(40)) + rand.Float64()
			lat = 18.2423 + float64(rand.Intn(30)) + rand.Float64()

			_, _, err := db.Set(k, lng, lat)
			if err != nil {
				fmt.Println("err " + err.Error())
			}
			if i%10000 == 0 {
				fmt.Printf("i %d \n", i)
			}
		}
	}
	fmt.Printf("db count %d\n", db.Count())
	fmt.Println("done")

	//query bench
	max = 1000000
	var items []Item
	var c int
	for {
		i = 0
		c = 0
		t2 := time.Now()
		for i < max{
			i++
			lng = 90.123 + float64(rand.Intn(40)) + rand.Float64()
			lat = 18.2423 + float64(rand.Intn(30)) + rand.Float64()

			items = Nearby(db, lng, lat, 500, 10)
			//fmt.Printf("near items %d\n", len(items))
			c += len(items)
		}
		fmt.Printf("query %d times in %s, total items %d\n", max, time.Since(t2), c)
	}

}

func watchOutOfMemory() {
	if gogc != -1 {
		debug.SetGCPercent(gogc)
		return
	}

	debug.SetGCPercent(-1)
	t := time.NewTicker(time.Second * time.Duration(gcs))
	defer t.Stop()
	//var mem runtime.MemStats
	//var oom = false
	for range t.C {
		runtime.GC()
		//runtime.ReadMemStats(&mem)
		//oom = int(mem.HeapAlloc) > 2 * 1024 * 1024 * 1024
		//if oom {
		//	runtime.GC()
		//	log.Info("watch oom gc", zap.Any("healAlloc", mem.HeapAlloc))
		//}
	}
}

func Nearby(db *poidb.DB, lng, lat float64, meters float64, limit int) (items []Item) {
	p := geometry.Point{lng, lat}
	target := geojson.NewCircle(p, meters, 64)

	maxDist := target.Haversine()
	db.Nearby(target, nil, nil, func(id string, lng, lat float64) bool {
		dist := target.HaversineTo(geometry.Point{lng, lat})
		if maxDist > 0 && dist > maxDist {
			return false
		}
		items = append(items, Item{
			Id:   id,
			X:    lng,
			Y:    lat,
			Dist: geo.DistanceFromHaversine(dist),
		})
		return len(items) < limit
	})
	sort.Slice(items, func(i, j int) bool {
		return items[i].Dist < items[j].Dist
	})
	return
}
