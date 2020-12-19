package ebest_server

import (
	"github.com/sangx2/ebest"
	"github.com/sangx2/ebest/res"
	"github.com/sangx2/ebest_server/model"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
)

func TestEBest(t *testing.T) {
	// fix me
	server := NewServer("id", "password", "certPassword",
		ebest.ServerVirtual, "C:\\eBEST\\xingAPI\\Res\\", 1000)

	if e := server.Connect(); e != nil {
		t.Errorf("Connect error:%s", e.Error())
		return
	}
	defer server.Disconnect()

	if e := server.Login(); e != nil {
		t.Errorf("Login error:%s", e.Error())
		return
	}

	accounts := server.GetAccountList()
	for i, account := range accounts {
		t.Logf("%dth GetAccount:%s", i+1, account)
		t.Logf("%dth GetAccountName:%s", i+1, server.GetAccountName(accounts[i]))
		t.Logf("%dth GetAccountDetailName:%s", i+1, server.GetAccountDetailName(accounts[i]))
		t.Logf("%dth GetAccountNickName:%s", i+1, server.GetAccountNickName(accounts[i]))
	}

	if e := server.StartQueryServer(); e != nil {
		t.Errorf("StartQueryServer error:%s", e.Error())
		return
	}

	// query
	reqQuery := model.NewRequest(ebest.T0424, res.T0424InBlock{Accno: accounts[0]}, false, 1)
	e := server.RequestQuery(reqQuery)
	if e != nil {
		t.Errorf("RequestQuery:T0424:%s", e.Error())
		return
	} else {
		if resp := <-reqQuery.RespChan; resp.Error != nil {
			t.Errorf("RequestQuery:T0424:%s", resp.Error.Error())
		} else {
			t.Logf("RequestQuery:T0424:%+v", resp.OutBlocks)
		}
	}

	// real
	reqReal := model.NewRequest(ebest.NWS, res.NWSInBlock{NWcode: "NWS001"}, false, 100)
	e = server.StartRealServerWithRequest(reqReal)
	if e != nil {
		t.Errorf("CreateReal:NWS:%s", e.Error())
		return
	}

	interruptChan := make(chan os.Signal, 1)
	signal.Notify(interruptChan, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

	var wg sync.WaitGroup
	wg.Add(1)
	nwsDoneChan := make(chan bool, 1)
	go func(chan bool) {
		for {
			select {
			case resp := <-reqReal.RespChan:
				if resp.Error != nil {
					t.Errorf("StartRealServerWithRequestReal:NWS:%s", resp.Error.Error())
				} else {
					t.Logf("StartRealServerWithRequestReal:Response:%+v", resp.OutBlocks)
				}
			case <-nwsDoneChan:
				wg.Done()
				return
			}
		}
	}(nwsDoneChan)

	select {
	case <-interruptChan:
		nwsDoneChan <- true
	}

	wg.Wait()

	server.ShutdownServer()
	server.Disconnect()
}
