package main

import (
	"fmt"
	"testing"

	"github.com/bozkayasalih01x/go-event/store"
	"github.com/bozkayasalih01x/go-event/tester"
)

func TestMain(t *testing.T) {
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
