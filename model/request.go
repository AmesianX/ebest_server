package model

type Request struct {
	ResName string
	InBlock interface{}

	IsOccurs bool

	RespChan chan *Response
}

func NewRequest(resName string, inBlock interface{}, isOccurs bool, respChanSize int) *Request {
	return &Request{
		ResName:  resName,
		InBlock:  inBlock,
		IsOccurs: isOccurs,
		RespChan: make(chan *Response, respChanSize),
	}
}
