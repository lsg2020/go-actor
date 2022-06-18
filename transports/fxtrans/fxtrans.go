package fxtrans

import (
	"strings"
	"sync"

	"github.com/golang/protobuf/proto"
	goactor "github.com/lsg2020/go-actor"
	message "github.com/lsg2020/go-actor/pb"
	"go.uber.org/zap"
)

func NewFx(service string, node string, sendMsg func(string, string, []byte) error) *FxTransport {
	trans := &FxTransport{
		service: service,
		node:    node,
		sendMsg: sendMsg,
	}
	return trans
}

type FxTransport struct {
	system  *goactor.ActorSystem
	service string
	node    string
	sendMsg func(string, string, []byte) error

	nodeMutex sync.Mutex
	services  map[uint64]string
	nodes     map[uint64]string

	responseMutex sync.Mutex
	responses     map[int32]*goactor.DispatchMessage
	session       int32
}

func (trans *FxTransport) Name() string {
	return "fxtrans"
}

func (trans *FxTransport) URI() string {
	return trans.service + ":" + trans.node
}

func (trans *FxTransport) Init(system *goactor.ActorSystem) error {
	trans.services = make(map[uint64]string)
	trans.nodes = make(map[uint64]string)
	trans.responses = make(map[int32]*goactor.DispatchMessage)
	trans.session = 1
	trans.system = system
	return nil
}

func (trans *FxTransport) ReceiveCB() func(buf []byte) {
	response := func(msg *goactor.DispatchMessage, err error, data interface{}) {
		headers := goactor.Headers{
			goactor.BuildHeaderInt(goactor.HeaderIdProtocol, goactor.ProtocolResponse),
			goactor.BuildHeaderInt(goactor.HeaderIdSession, msg.SessionId),
			goactor.BuildHeaderInt(goactor.HeaderIdTransSession, msg.Headers.GetInt(goactor.HeaderIdTransSession)),
		}
		responseMsg := goactor.NewDispatchMessage(nil, nil, nil, goactor.ProtocolResponse, msg.SessionId, headers, data, err, nil)

		buf, perr := proto.Marshal(responseMsg.ToPB())
		if perr != nil {
			trans.system.Logger().Error("response pack error", zap.Error(perr))
			return
		}

		err = trans.sendMsg(msg.Headers.GetStr(goactor.HeaderIdTransAddress2), msg.Headers.GetStr(goactor.HeaderIdTransAddress), buf)
		if err != nil {
			trans.system.Logger().Error("fx transport response error", zap.Error(err))
		}
	}

	return func(buf []byte) {
		msg := &message.Message{}
		err := proto.Unmarshal(buf, msg)
		if err != nil {
			trans.system.Logger().Error("receive invalid pack ", zap.Error(err))
			return
		}
		dispatch := goactor.NewDispatchMessageFromPB(msg, response)

		if dispatch.ProtocolId == goactor.ProtocolResponse {
			var requestMsg *goactor.DispatchMessage
			trans.responseMutex.Lock()
			requestMsg = trans.responses[int32(dispatch.Headers.GetInt(goactor.HeaderIdTransSession))]
			trans.responseMutex.Unlock()
			if requestMsg != nil {
				requestMsg.DispatchResponse(dispatch, dispatch.ResponseErr, dispatch.Content)
			}
		} else {
			session := dispatch.SessionId
			if session == 0 {
				dispatch.DispatchResponse = nil
			}

			err = trans.system.Dispatch(dispatch)
			if err != nil {
				if session != 0 {
					dispatch.Response(err, nil)
				}
				trans.system.Logger().Error("dispatch message error", zap.Error(err))
			}
		}
	}
}

func (trans *FxTransport) Send(msg *goactor.DispatchMessage) (goactor.SessionCancel, error) {
	destination := msg.Destination
	if destination == nil {
		return nil, goactor.ErrNeedDestination
	}
	trans.nodeMutex.Lock()
	serviceName := trans.services[destination.NodeInstanceId]
	nodeName := trans.nodes[destination.NodeInstanceId]
	if nodeName == "" {
		cfgNode := trans.system.NodeConfig(destination.NodeInstanceId)
		if cfgNode == nil {
			trans.nodeMutex.Unlock()
			return nil, goactor.ErrorWrapf(goactor.ErrNodeMiss, "node:%d", destination.NodeInstanceId)
		}
		nodeAddr, ok := cfgNode.Transports[trans.Name()]
		if !ok {
			trans.nodeMutex.Unlock()
			return nil, goactor.ErrorWrapf(goactor.ErrTransportMiss, "trans:%s", trans.Name())
		}
		names := strings.Split(nodeAddr, ":")
		serviceName = names[0]
		nodeName = names[1]
		trans.services[destination.NodeInstanceId] = names[0]
		trans.nodes[destination.NodeInstanceId] = names[1]
	}
	trans.nodeMutex.Unlock()

	reqsession := msg.SessionId
	transSession := int32(0)
	if reqsession != 0 {
		trans.responseMutex.Lock()
		trans.session++
		transSession = trans.session
		msg.Headers.Put(
			goactor.BuildHeaderInt(goactor.HeaderIdTransSession, int(transSession)),
			goactor.BuildHeaderString(goactor.HeaderIdTransAddress2, trans.service),
			goactor.BuildHeaderString(goactor.HeaderIdTransAddress, trans.node),
		)

		trans.responses[transSession] = msg
		trans.responseMutex.Unlock()
	}

	buf, err := proto.Marshal(msg.ToPB())
	if err != nil {
		return nil, goactor.ErrorWrapf(err, "pb marshal error")
	}

	err = trans.sendMsg(serviceName, nodeName, buf)
	if err != nil {
		return nil, goactor.ErrorWrapf(err, "service:%s node:%s", serviceName, nodeName)
	}

	if reqsession != 0 {
		return func() {
			trans.responseMutex.Lock()
			delete(trans.responses, transSession)
			trans.responseMutex.Unlock()
		}, nil
	}
	return nil, nil
}
