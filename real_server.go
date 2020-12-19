package ebest_server

import (
	"fmt"
	"github.com/sangx2/ebest"
	"github.com/sangx2/ebest/impl"
	"github.com/sangx2/ebest/interfaces"
	"github.com/sangx2/ebest/res"
	"github.com/sangx2/ebest_server/model"
	"sync"
	"time"
)

type RealServer struct {
	resPath string

	reals map[string]*ebest.Real

	respQSize int
	respChans map[string]chan *model.Response

	recvDoneChans map[string]chan bool

	gracefulShutdownTimeDelay time.Duration

	*ebest.EBest

	wg sync.WaitGroup
}

func NewRealServer(resPath string, respQSize int, eBest *ebest.EBest) *RealServer {
	return &RealServer{
		resPath: resPath,

		reals: make(map[string]*ebest.Real),

		respQSize: respQSize,
		respChans: make(map[string]chan *model.Response),

		recvDoneChans: make(map[string]chan bool),

		EBest: eBest,
	}
}

func (r *RealServer) StartRealServerWithRequest(request *model.Request) error {
	key := request.ResName

	var trade interfaces.RealTrade

	switch request.ResName {
	case ebest.H1:
		if h1InBlock, ok := request.InBlock.(res.H1InBlock); ok {
			key += ":" + h1InBlock.Shcode
			trade = impl.NewH1()
		}
	case ebest.HA:
		if haInBlock, ok := request.InBlock.(res.HAInBlock); ok {
			key += ":" + haInBlock.Shcode
			trade = impl.NewHA()
		}
	case ebest.S3:
		if s3InBlock, ok := request.InBlock.(res.S3InBlock); ok {
			key += ":" + s3InBlock.Shcode
			trade = impl.NewS3()
		}
	case ebest.K3:
		if k3InBlock, ok := request.InBlock.(res.K3InBlock); ok {
			key += ":" + k3InBlock.Shcode
			trade = impl.NewK3()
		}
	case ebest.K1:
		if k1InBlock, ok := request.InBlock.(res.K1InBlock); ok {
			key += ":" + k1InBlock.Shcode
			trade = impl.NewK1()
		}
	case ebest.OK:
		if okInBlock, ok := request.InBlock.(res.OKInBlock); ok {
			key += ":" + okInBlock.Shcode
			trade = impl.NewOK()
		}
	case ebest.SC0:
		trade = impl.NewSC0()
	case ebest.SC1:
		trade = impl.NewSC1()
	case ebest.SC2:
		trade = impl.NewSC2()
	case ebest.SC3:
		trade = impl.NewSC3()
	case ebest.SC4:
		trade = impl.NewSC4()
	case ebest.NWS:
		if nwsInBlock, ok := request.InBlock.(res.NWSInBlock); ok {
			key += ":" + nwsInBlock.NWcode
			trade = impl.NewNWS()
		}
	default:
		return fmt.Errorf("invalid ResName")
	}
	if trade == nil {
		return fmt.Errorf("invalid InBlock")
	}

	if _, isExist := r.reals[key]; isExist {
		return fmt.Errorf("%s is already exist", key)
	}

	r.respChans[key] = request.RespChan

	// create real
	real := ebest.NewReal(r.resPath, trade)
	if real == nil {
		return fmt.Errorf("CreateReal is nil")
	}
	r.reals[key] = real

	// set inBlock
	if e := real.SetInBlock(request.InBlock); e != nil {
		return e
	}

	// start real
	real.Start()

	// make receiver done channel
	receiverDoneChan := make(chan bool, 1)
	r.recvDoneChans[key] = receiverDoneChan

	createDoneChan := make(chan error, 1)
	r.wg.Add(1)
	go func(responseChan chan *model.Response, createDoneChan chan error, receiverDoneChan chan bool) {
		createDoneChan <- nil
		close(createDoneChan)
		for {
			select {
			case <-real.GetReceivedRealDataChan():
				responseChan <- model.NewResponse([]interface{}{real.GetOutBlock()}, nil)
			case <-receiverDoneChan:
				r.wg.Done()
				return
			}
		}
	}(request.RespChan, createDoneChan, receiverDoneChan)
	<-createDoneChan

	return nil
}

func (r *RealServer) ShutdownServer() {
	for _, real := range r.reals {
		real.Close()
	}

	for _, recvDoneChan := range r.recvDoneChans {
		recvDoneChan <- true
		close(recvDoneChan)
	}
	r.wg.Wait()
}
