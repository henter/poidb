package main

import (
	"fmt"
	"github.com/henter/poidb"
	"github.com/rs/xid"
	"github.com/tidwall/geojson"
	"github.com/tidwall/geojson/geo"
	"github.com/tidwall/geojson/geometry"
	"math/rand"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"
)

type Item struct {
	Id         string
	X, Y, Dist float64
}

func main() {
	//db, err := poidb.Open(":memory:")
	t := time.Now()
	db, err := poidb.Open("/tmp/poi.db")
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

	max := 80000000
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
		}
	}
	fmt.Printf("db count %d\n", db.Count())
	fmt.Println("done")

	//query bench
	max = 100000
	i = 0
	var items []Item
	t2 := time.Now()
	for i < max {
		i++
		lng = 90.123 + float64(rand.Intn(40)) + rand.Float64()
		lat = 18.2423 + float64(rand.Intn(30)) + rand.Float64()

		items = Nearby(db, lng, lat, 500, 10)
		fmt.Printf("near items %d\n", len(items))
	}
	fmt.Printf("query %d times in %s\n", max, time.Since(t2))
	select {}
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
