package ebest_server

import (
	"fmt"
	"github.com/sangx2/ebest"
	"github.com/sangx2/ebest/impl"
	"github.com/sangx2/ebest/interfaces"
	"github.com/sangx2/ebest/res"
	"github.com/sangx2/ebest_server/model"
	"sync"
	"sync/atomic"
	"time"
)

type QueryServer struct {
	resPath string

	querys map[string]*ebest.Query

	reqQSize int
	reqChans map[string]chan *model.Request

	recvDoneChans map[string]chan bool

	gracefulShutdownTimeDelay time.Duration

	*ebest.EBest

	wg sync.WaitGroup
}

func NewQueryServer(resPath string, reqQSize int, eBest *ebest.EBest) *QueryServer {
	return &QueryServer{
		resPath: resPath,

		querys: make(map[string]*ebest.Query),

		reqQSize: reqQSize,
		reqChans: make(map[string]chan *model.Request),

		recvDoneChans: make(map[string]chan bool),

		EBest: eBest,
	}
}

func (q *QueryServer) Request(request *model.Request) error {
	var ok bool
	switch request.ResName {
	case ebest.CSPAQ12200:
		_, ok = request.InBlock.(res.CSPAQ12200InBlock1)
	case ebest.CSPAT00600:
		_, ok = request.InBlock.(res.CSPAT00600InBlock1)
	case ebest.CSPAT00700:
		_, ok = request.InBlock.(res.CSPAT00700InBlock1)
	case ebest.CSPAT00800:
		_, ok = request.InBlock.(res.CSPAT00800InBlock1)
	case ebest.T0424:
		_, ok = request.InBlock.(res.T0424InBlock)
	case ebest.T1101:
		_, ok = request.InBlock.(res.T1101InBlock)
	case ebest.T1305:
		_, ok = request.InBlock.(res.T1305InBlock)
	case ebest.T1511:
		_, ok = request.InBlock.(res.T1511InBlock)
	case ebest.T3320:
		_, ok = request.InBlock.(res.T3320InBlock)
	case ebest.T8424:
		_, ok = request.InBlock.(res.T8424InBlock)
	case ebest.T8436:
		_, ok = request.InBlock.(res.T8436InBlock)
	default:
		return fmt.Errorf("invalid ResName")
	}
	if !ok {
		return fmt.Errorf("invalid InBlock")
	}
	q.reqChans[request.ResName] <- request

	return nil
}

func (q *QueryServer) StartQueryServer() error {
	resNames := []string{
		ebest.CSPAQ12200,
		ebest.CSPAT00600,
		ebest.CSPAT00700,
		ebest.CSPAT00800,
		ebest.T0424,
		ebest.T1101,
		ebest.T1305,
		ebest.T1511,
		ebest.T3320,
		ebest.T8424,
		ebest.T8436,
	}

	for _, resName := range resNames {
		if _, isExist := q.querys[resName]; isExist {
			return fmt.Errorf("%s is exist", resName)
		}

		var trade interfaces.QueryTrade
		switch resName {
		case ebest.CSPAQ12200:
			trade = impl.NewCSPAQ12200()
		case ebest.CSPAT00600:
			trade = impl.NewCSPAT00600()
		case ebest.CSPAT00700:
			trade = impl.NewCSPAT00700()
		case ebest.CSPAT00800:
			trade = impl.NewCSPAT00800()
		case ebest.T1101:
			trade = impl.NewT1101()
		case ebest.T1305:
			trade = impl.NewT1305()
		case ebest.T1511:
			trade = impl.NewT1511()
		case ebest.T3320:
			trade = impl.NewT3320()
		case ebest.T0424:
			trade = impl.NewT0424()
		case ebest.T8424:
			trade = impl.NewT8424()
		case ebest.T8436:
			trade = impl.NewT8436()
		default:
			return fmt.Errorf("invalid ResName")
		}

		// make request channel
		q.reqChans[resName] = make(chan *model.Request, q.reqQSize)

		// create query
		query := ebest.NewQuery(q.resPath, trade)
		q.querys[resName] = query

		// make receiver done channel
		q.recvDoneChans[resName] = make(chan bool, 1)

		// create receiver
		q.createReceiver(resName, query)
	}

	return nil
}

func (q *QueryServer) createReceiver(resName string, query *ebest.Query) {
	createDoneChan := make(chan error, 1)
	defer close(createDoneChan)

	// create receiver
	q.wg.Add(1)
	go func(resName string, query *ebest.Query, requestChan chan *model.Request, createDoneChan chan error, receiverDoneChan chan bool) {
		defer q.wg.Done()

		remainingTPS := int32(query.TPS)
		remainingLPP := int32(query.LPP)

		var tpsEnableChan chan bool
		var lppEnableChan chan bool
		var tpsDoneChan chan bool
		defer func() {
			if tpsDoneChan != nil {
				tpsDoneChan <- true
				close(tpsDoneChan)
			}
		}()
		var lppDoneChan chan bool
		defer func() {
			if lppDoneChan != nil {
				lppDoneChan <- true
				close(lppDoneChan)
			}
		}()

		createDoneChan <- nil
		for {
			// 초당 요청 제한
			if query.TPS != -1 { // 제한이 있을 경우
				if atomic.LoadInt32(&remainingTPS) == 0 {
					select {
					case <-tpsEnableChan: // 요청이 가능할 때까지 대기
						atomic.StoreInt32(&remainingTPS, int32(query.TPS))
					case <-receiverDoneChan:
						return
					}
				}
			}
			// 10분당 요청 제한
			if query.LPP != -1 { // 제한이 있을 경우
				if atomic.LoadInt32(&remainingLPP) == 0 {
					select {
					case <-lppEnableChan: // 요청이 가능할 때까지 대기
						atomic.StoreInt32(&remainingLPP, int32(query.LPP))
					case <-receiverDoneChan:
						return
					}
				}
			}

			select {
			case request := <-requestChan:
				// query
				if e := query.SetInBlock(request.InBlock); e != nil {
					request.RespChan <- model.NewResponse(nil, fmt.Errorf("Query.SetInBlock:%s:%d", resName, e))
					break
				}

				ret := query.Request(request.IsOccurs)
				if ret < 0 {
					request.RespChan <- model.NewResponse(nil, fmt.Errorf("Query.Request:%s", q.EBest.GetErrorMessage(ret)))
					break
				}

				msg, e := query.GetReceiveMessage()
				if e != nil {
					request.RespChan <- model.NewResponse(nil, fmt.Errorf("Query.GetReceiveMessage:%s:%+v", resName, e))
					break
				} else {
					switch msg {
					case "00000:자료조회중 오류발생":
						request.RespChan <- model.NewResponse(nil, fmt.Errorf("Query.GetReceiveMessage:%s:%s", resName, msg))
					default:
						<-query.GetReceiveDataChan()

						request.RespChan <- model.NewResponse(query.GetOutBlocks(), nil)
					}
				}

				// 초당 요청 제한을 스케쥴링
				if query.TPS != -1 { // 제한이 있을 경우 경우
					if atomic.LoadInt32(&remainingTPS) == int32(query.TPS) {
						tpsDoneChan, tpsEnableChan = q.scheduling(time.NewTimer(time.Second * 1))
					}
					atomic.AddInt32(&remainingTPS, -1)
				}
				// 10분당 요청 제한을 스케쥴링
				if query.LPP != -1 { // 제한이 있을 경우
					if atomic.LoadInt32(&remainingLPP) == int32(query.LPP) {
						lppDoneChan, lppEnableChan = q.scheduling(time.NewTimer(time.Minute*10 + time.Microsecond*10))
					}
					atomic.AddInt32(&remainingLPP, -1)
				}
			case <-receiverDoneChan:
				return
			}
		}
	}(resName, query, q.reqChans[resName], createDoneChan, q.recvDoneChans[resName])
	<-createDoneChan
}

func (q *QueryServer) scheduling(timer *time.Timer) (chan bool, chan bool) {
	createDone := make(chan error, 1)
	defer close(createDone)

	doneChan := make(chan bool, 1)
	enableChan := make(chan bool, 1)

	q.wg.Add(1)
	go func() {
		defer q.wg.Done()

		createDone <- nil
		select {
		case <-timer.C:
			enableChan <- true
			close(enableChan)
		case <-doneChan:
			timer.Stop()
		}
	}()
	<-createDone

	return doneChan, enableChan
}

func (q *QueryServer) ShutdownServer() {
	for resName, requestChan := range q.reqChans {
		go func(query *ebest.Query, requestChan chan *model.Request, receiverDoneChan chan bool) {
			for len(requestChan) != 0 {
				<-requestChan
			}

			receiverDoneChan <- true
			close(receiverDoneChan)

			query.Close()
		}(q.querys[resName], requestChan, q.recvDoneChans[resName])
	}
	q.wg.Wait()
}
