package ebest_server

import (
	"github.com/sangx2/ebest"
	"github.com/sangx2/ebest_server/model"
)

type Server struct {
	queryServer *QueryServer
	realServer  *RealServer

	*ebest.EBest
}

func NewServer(id, password, certPassword, server, resPath string, queueSize int) *Server {
	if eBest := ebest.NewEBest(id, password, certPassword, server, ebest.Port, resPath); eBest != nil {
		return &Server{
			queryServer: NewQueryServer(resPath, queueSize, eBest),
			realServer:  NewRealServer(resPath, queueSize, eBest),

			EBest: eBest,
		}
	}
	return nil
}

func (s *Server) Connect() error {
	return s.EBest.Connect()
}

func (s *Server) Login() error {
	return s.EBest.Login()
}

func (s *Server) GetAccountList() []string {
	return s.EBest.GetAccountList()
}

func (s *Server) GetAccountName(account string) string {
	return s.EBest.GetAccountName(account)
}

func (s *Server) GetAccountDetailName(account string) string {
	return s.EBest.GetAccountDetailName(account)
}

func (s *Server) GetAccountNickName(account string) string {
	return s.EBest.GetAccountNickName(account)
}

// query
func (s *Server) StartQueryServer() error {
	return s.queryServer.StartQueryServer()
}

func (s *Server) RequestQuery(request *model.Request) error {
	return s.queryServer.Request(request)
}

// real
func (s *Server) StartRealServerWithRequest(request *model.Request) error {
	return s.realServer.StartRealServerWithRequest(request)
}

func (s *Server) ShutdownServer() {
	s.queryServer.ShutdownServer()
	s.realServer.ShutdownServer()
}

func (s *Server) Disconnect() {
	s.EBest.Disconnect()
}
