package main

import (
	"container/list"
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/bozkayasalih01x/go-event/store"
	"github.com/bozkayasalih01x/go-event/tester"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var URL string
var customerAndGameUrl string
var gameAndCustomers string

func init() {
	rand.Seed(time.Now().UnixNano())
	err := godotenv.Load("app.env")

	if err != nil {
		panic(err)
	}

	URL = os.Getenv("GEARBOX_URL")
	customerAndGameUrl = os.Getenv("MONGO_CUSTOMER_AND_GAME_URL")
	gameAndCustomers = os.Getenv("GAME_AND_CUSTOMERS")
}

func (conn *Conn) GameID(versionId string) (Game, error) {
	game := Game{}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	val, ok := conn.gameStore[versionId]
	if !ok {
		return game, nil
	}
	game.GameID = val
	return game, nil
}

func (conn *Conn) CustomerID(versionId string) (Customer, error) {
	customer := Customer{}
	conn.mu.Lock()
	defer conn.mu.Unlock()
	val, ok := conn.customerStore[versionId]
	if !ok {
		return customer, nil
	}
	customer.CustomerID = val
	return customer, nil

}

type Conn struct {
	client        *mongo.Database
	mu            sync.RWMutex
	ctx           context.Context
	gameStore     map[string]string
	store         map[string]SettledType
	customerStore map[string]string
	signal        chan int
	queue         *list.List
}

func NewConnection() *Conn {
	var DATABASE string = os.Getenv("DATABASE")
	var MONGO_URL string = os.Getenv("MONGO_URL")
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(MONGO_URL))
	if err != nil {
		log.Fatalf("couldn't connect to mongo %v", err)
	}

	db := client.Database(DATABASE)

	c := &Conn{
		ctx:           ctx,
		client:        db,
		mu:            sync.RWMutex{},
		signal:        make(chan int, 100),
		store:         make(map[string]SettledType),
		customerStore: make(map[string]string),
		gameStore:     make(map[string]string),
	}
	c.queue = list.New()
	return c

}

type FetcherType struct {
	Game     string `bson:"game"`
	Customer string `bson:"customer"`
	ID       string `bson:"_id"`
}

func (conn *Conn) preFetcher(col *mongo.Collection, c context.Context) {
	opts := bson.D{{Key: "game", Value: 1}, {Key: "customer", Value: 1}, {Key: "_id", Value: 1}}
	cur, err := col.Find(c, bson.D{}, &options.FindOptions{
		Projection: opts,
	})
	if err != nil {
		log.Fatalf("couldn't find anything on mongodb %v\n", err)
	}
	var res FetcherType
	for cur.Next(c) {
		err := cur.Decode(&res)
		if err != nil {
			log.Fatalf("couldn't fetch all data properly %v\n", err)
		}
		conn.gameStore[res.ID] = res.Game
		conn.customerStore[res.ID] = res.Customer
	}
}

func main() {
	server := NewConnection()
	customerAndGameClient, c := store.NewClient(customerAndGameUrl)
	gameAndCustomerCollection := customerAndGameClient.Database("results").Collection(gameAndCustomers)

	fmt.Println("prefetching..")
	server.preFetcher(gameAndCustomerCollection, c)
	fmt.Println("prefetching done..")

	c, d := tester.RunCollection()

	go server.Listener()
	server.Runner(c, d)
	defer server.client.Client().Disconnect(server.ctx)
	defer d.Client().Disconnect(c)
}

func (conn *Conn) Runner(ctx context.Context, db *mongo.Database) {
	aggrCol := db.Collection("aggragatedResults")
	for msg := range conn.signal {
		fmt.Printf("msg %v\n", msg)

		element := conn.queue.Back()
		if element != nil {
			go conn.looper(element.Value.(string))
			conn.queue.Remove(element)
		}
		conn.mu.Lock()
		for _, val := range conn.store {
			_, err := aggrCol.InsertOne(ctx, val)
			if err != nil {
				f := bson.M{"_id": IDType{
					Version:   val.ID.Version,
					Network:   val.ID.Network,
					Os:        val.ID.Os,
					Event:     val.ID.Event,
					Timestamp: val.ID.Timestamp,
					TimeSpan:  val.ID.TimeSpan,
				}}
				e := aggrCol.FindOneAndUpdate(ctx, f, bson.M{"$set": val}).Err()
				if e != nil {
					fmt.Printf("another type error: %v\n", err)
				}
			}
		}

		if conn.queue.Len() == 0 {
			fmt.Println("all done...")
		}
		conn.mu.Unlock()
	}

}

type RawType struct {
	ID        primitive.ObjectID `bson:"_id"`
	Version   string             `bson:"version"`
	Network   string             `bson:"network"`
	Event     string             `bson:"event"`
	Os        string             `bson:"os,omitempty"`
	Value     float64            `bson:"value,omitempty"`
	Session   string             `bson:"session,omitempty"`
	Ip        string             `bson:"ip,omitempty"`
	Time      float64            `bson:"time,omitempty"`
	Heatmap   primitive.D        `bson:"heatmap,omitempty"`
	Timestamp primitive.DateTime `bson:"timestamp,omitempty"`
}

type IDType struct {
	Version   string `bson:"version"`
	Network   string `bson:"network"`
	Event     string `bson:"event"`
	Os        string `bson:"os"`
	Timestamp string `bson:"timestamp"`
	TimeSpan  int    `bson:"timespan"`
}

type SettledType struct {
	ID       IDType      `bson:"_id"`
	Customer string      `bson:"customer"`
	Game     string      `bson:"game"`
	Value    interface{} `bson:"value"`
}

type RawVersion struct {
	Version string `bson:"version"`
}

type RawNetwork struct {
	Network string `bson:"network"`
}

func (conn *Conn) looper(curCol string) {
	cursor, err := conn.client.Collection(curCol).Find(conn.ctx, bson.M{})
	if err != nil {
		fmt.Printf("couldn't connect %s collections: %v\n", curCol, err)
	}

	fmt.Printf("current collection %s\n", curCol)
	defer cursor.Close(conn.ctx)
	start := time.Now()
	var result RawType
	var i int
	for cursor.Next(conn.ctx) {
		err := cursor.Decode(&result)
		if err != nil {
			log.Fatalf("couldn't read current data %s with error: %v\n", curCol, err)
		}
		resp, _ := conn.GameID(result.Version)
		res, _ := conn.CustomerID(result.Version)
		i++
		if resp.GameID != "" && res.CustomerID != "" {
			conn.switchHandler(result, resp.GameID, res.CustomerID)
		}

	}
	fmt.Printf("took, %v\n", time.Since(start).Minutes())
	fmt.Printf("total %d\n", i)
	conn.signal <- 1
	if err := cursor.Err(); err != nil {
		log.Fatal(err)
	}
}

type Compose struct {
	RawNetwork
	RawVersion
}

func (m *Conn) AggragateEvent(data RawType, keys []string, game string, customer string, oldEvent bool, event ...string) {
	if len(event) >= 2 {
		panic("couldnt set more than one optinal event")
	}
	day := DateFormat(data.Timestamp.Time())
	key := fmt.Sprintf("%s::%s::%s::%s", data.Version, data.Network, data.Os, day)
	var optionalEvent string
	if event[0] != "" {
		optionalEvent = event[0]
	} else {
		optionalEvent = data.Event
	}
	key = fmt.Sprintf("%s::%s", key, optionalEvent)

	m.mu.Lock()
	defer m.mu.Unlock()
	val, ok := m.store[key]
	if !ok {
		s := SettledType{
			ID: IDType{
				Version:   data.Version,
				Timestamp: day,
				Os:        data.Os,
				Network:   data.Network,
				TimeSpan:  1440,
				Event:     optionalEvent,
			},
			Game:     game,
			Customer: customer,
			Value:    0,
		}
		m.store[key] = s
		val = s
	}
	var ev string
	if event[0] != "" {
		ev = event[0]
	} else {
		ev = data.Event
	}

	if val.Value != nil {
		intValue := val.Value.(int)
		if !oldEvent && strings.Contains(ev, "Time") && ev != "totalTime" {
			intValue += int(data.Time)
			val.Value = intValue
			m.store[key] = val
		} else if oldEvent && strings.Contains(ev, "Time") {
			intValue += int(data.Value)
			val.Value = intValue
			m.store[key] = val
		} else {
			intValue += 1
			val.Value = intValue
			m.store[key] = val
		}
	}

}

func (conn *Conn) switchHandler(data RawType, game string, customer string) {
	switch data.Event {
	case "platform":
		break
	case "click":
		conn.handleClick(data, game, customer)
	case "cta":
	case "ctaClick":
		conn.handleCtaClick(data, game, customer)
	case "end":
		if data.Value == 0 {
			conn.AggragateEvent(data, []string{"network", "version"}, game, customer, false, "gameWon")
		} else if data.Value == 1 {
			conn.AggragateEvent(data, []string{"network", "version"}, game, customer, false, "gameLoss")
		} else {
			conn.AggragateEvent(data, []string{"network", "version"}, game, customer, false, "gameUnknownOutcome")
		}
		conn.AggragateEvent(data, []string{"network", "version"}, game, customer, false, "gameFinished")
	case "start":
		conn.AggragateEvent(data, []string{"network", "version"}, game, customer, false, "gameStarted")
		if data.Time <= 60 {
			conn.AggragateEvent(data, []string{"version", "network"}, game, customer, false, "gameStartedTime")
		}
	case "restart":
		conn.AggragateEvent(data, []string{"version", "network"}, game, customer, false, "gameRestarted")
	case "time":
		conn.AggragateEvent(data, []string{"version", "network"}, game, customer, false, "totalTime")
	case "impression":
		conn.AggragateEvent(data, []string{"version", "network"}, game, customer, false, "impression")
	case "gameStarted":
	case "gameStartTime":
	case "gameRestarted":
	case "firstClick":
	case "firstClickTime":
	case "gameFinished":
	case "endGameTime":
	case "gameWon":
	case "gameLose":
		conn.AggragateEvent(data, []string{"version", "network"}, game, customer, true, data.Event)
	default:
		conn.AggragateEvent(data, []string{"version", "network"}, game, customer, false, data.Event)
	}
}

type PortAndLand struct {
	Portrait  map[int32]map[int32]int32 `bson:"portrait"`
	Landscape map[int32]map[int32]int32 `bson:"landscape"`
}

func DateFormat(t time.Time) string {
	return fmt.Sprintf("%d-%02d-%02dT00:00:00.000+00:00", t.Year(), int(t.Month()), t.Day())
}

type HeatMap struct {
	X   float64    `bson:"x"`
	Y   float64    `bson:"y"`
	Dim [2]float64 `bson:"dim"`
}

type Game struct {
	GameID string `bson:"game"`
}

type Customer struct {
	CustomerID string `bson:"customer"`
}

type UserUnknown struct {
	User    float64 `bson:"user"`
	Auto    float64 `bson:"auto"`
	Unknown float64 `bson:"unknown"`
}

func (conn *Conn) handleClick(d RawType, game string, customer string) {
	if d.Value == 0 {
		conn.AggragateEvent(d, []string{"version", "network"}, game, customer, false, "firstClick")
		conn.AggragateEvent(d, []string{"version", "network"}, game, customer, false, "firstClickTime")
	}

	conn.mu.Lock()
	defer conn.mu.Unlock()

	if len(d.Heatmap) > 0 {
		if d.Time <= 60 {
			day := DateFormat(d.Timestamp.Time())
			key := fmt.Sprintf("%s::%s::%s::%s::heatmap", d.Version, d.Os, d.Network, day)

			val, ok := conn.store[key]
			if !ok {
				v := SettledType{
					ID: IDType{
						Version:   d.Version,
						Network:   d.Network,
						Os:        d.Os,
						Timestamp: day,
						Event:     "heatmap",
					},
					Game:     game,
					Customer: customer,
					Value:    PortAndLand{},
				}
				conn.store[key] = v
			}

			portAndLand := PortAndLand{}
			var x int32
			var y int32
			if d.Heatmap[0].Value.(int32) > 0 {
				x = d.Heatmap[0].Value.(int32) - 1
			}

			if d.Heatmap[1].Value.(int32) > 0 {
				y = d.Heatmap[1].Value.(int32)
			} else {
				y = d.Heatmap[1].Value.(int32) + d.Heatmap[2].Value.(primitive.A)[1].(int32)
			}
			cord := x + (y * d.Heatmap[2].Value.(primitive.A)[0].(int32))
			if d.Heatmap[0].Value.(int32) > 0 {
				if v, ok := portAndLand.Portrait[int32(d.Time)][cord]; ok {
					portAndLand.Portrait[int32(d.Time)][cord] = v + 1
				}
			} else {
				if v, ok := portAndLand.Landscape[int32(d.Time)][cord]; ok {
					portAndLand.Landscape[int32(d.Time)][cord] = v + 1
				}
			}
			val.Value = portAndLand
			conn.store[key] = val
		}
	}

}

func (conn *Conn) handleCtaClick(data RawType, gameId string, customerId string) {
	day := DateFormat(data.ID.Timestamp())
	key := fmt.Sprintf("%s::%s::%s::%s::ctaClick", data.Version, data.Os, data.Network, day)

	conn.mu.Lock()
	defer conn.mu.Unlock()

	v, ok := conn.store[key]
	if !ok {
		s := SettledType{
			ID: IDType{
				Version:   data.Version,
				Network:   data.Network,
				Timestamp: day,
				Os:        data.Os,
				TimeSpan:  1440,
				Event:     "ctaClick",
			},
			Value: UserUnknown{
				User:    0,
				Auto:    0,
				Unknown: 0,
			},
			Game:     gameId,
			Customer: customerId,
		}
		conn.store[key] = s
		v = s
	}

	if v.Value != nil {
		castedValue := v.Value.(UserUnknown)
		if data.Event == "cta" {
			if data.Value == 0 {
				castedValue.User += 1
			} else if data.Value == 1 {
				castedValue.Auto += 1
			}
		} else {
			castedValue.Unknown += 1
		}
		v.Value = castedValue
		conn.store[key] = v
	}

	if data.Time <= 60 && data.Event == "cta" {
		conn.AggragateEvent(data, []string{"version", "network"}, customerId, gameId, false, "ctaTime")
	}
}

func (c *Conn) Listener() {
	gen := bson.M{
		"name": bson.M{"$regex": primitive.Regex{
			Pattern: "x00*",
			Options: "i",
		}},
	}
	for {
		cols, err := c.client.ListCollectionNames(c.ctx, gen, &options.ListCollectionsOptions{
			NameOnly: options.ListCollections().NameOnly,
		})
		counter := 0
		for _, col := range cols {
			count, err := c.client.Collection(col).CountDocuments(c.ctx, bson.D{})
			if err != nil {
				fmt.Printf("couldn't get length of document: %v\n", err)
			}
			if count >= 1000000 {
				counter++
				c.queue.PushBack(col)
			}
		}

		if c.queue.Len() != 0 {
			cur := c.queue.Front()
			fmt.Println(cur.Value)
			go c.looper(cur.Value.(string))
			c.queue.Remove(cur)
		}

		if err != nil {
			panic(err)
		}
		time.Sleep(time.Hour * 1)
	}

}
