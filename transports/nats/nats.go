package nats

import (
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	goactor "github.com/lsg2020/go-actor"
	message "github.com/lsg2020/go-actor/pb"
	"github.com/nats-io/nats.go"
	"go.uber.org/zap"
)

func NewNats(servers string) *NatsTransport {
	trans := &NatsTransport{
		servers: servers,
	}
	return trans
}

type NatsTransport struct {
	system  *goactor.ActorSystem
	servers string
	topic   string

	nc *nats.Conn

	nodeMutex sync.Mutex
	nodes     map[uint64]string

	responseMutex sync.Mutex
	responses     map[int32]*goactor.DispatchMessage
	session       int32
}

func (trans *NatsTransport) Name() string {
	return "nats"
}

func (trans *NatsTransport) URI() string {
	return trans.topic
}

func (trans *NatsTransport) Init(system *goactor.ActorSystem) error {
	trans.nodes = make(map[uint64]string)
	trans.responses = make(map[int32]*goactor.DispatchMessage)
	trans.session = 1
	trans.system = system
	nc, err := nats.Connect(trans.servers)
	if err != nil {
		return err
	}

	node := fmt.Sprintf("%d", system.InstanceID())

	response := func(msg *goactor.DispatchMessage, err error, data interface{}) {
		headers := goactor.Headers{
			goactor.BuildHeaderInt(goactor.HeaderIdProtocol, goactor.ProtocolResponse),
			goactor.BuildHeaderInt(goactor.HeaderIdSession, msg.SessionId),
			goactor.BuildHeaderInt(goactor.HeaderIdTransSession, msg.Headers.GetInt(goactor.HeaderIdTransSession)),
		}
		responseMsg := goactor.NewDispatchMessage(nil, nil, nil, goactor.ProtocolResponse, msg.SessionId, headers, data, err, nil)

		buf, perr := proto.Marshal(responseMsg.ToPB())
		if perr != nil {
			system.Logger().Error("response pack error", zap.Error(perr))
			return
		}
		perr = nc.Publish(msg.Headers.GetStr(goactor.HeaderIdTransAddress), buf)
		if err != nil {
			system.Logger().Error("response pack error", zap.Error(perr))
		}
	}

	trans.nc = nc
	trans.topic = "node_" + node
	_, err = nc.Subscribe(trans.topic, func(m *nats.Msg) {
		msgBuf := m.Data

		msg := &message.Message{}
		err = proto.Unmarshal(msgBuf, msg)
		if err != nil {
			system.Logger().Error("receive invalid pack", zap.Error(err))
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

			err := trans.system.Dispatch(dispatch)
			if err != nil {
				if session != 0 {
					dispatch.Response(err, nil)
				}

				system.Logger().Error("dispatch message error", zap.Error(err))
			}
		}
	})

	return err
}

func (trans *NatsTransport) Send(msg *goactor.DispatchMessage) (goactor.SessionCancel, error) {
	destination := msg.Destination
	if destination == nil {
		return nil, goactor.ErrNeedDestination
	}

	trans.nodeMutex.Lock()
	nodeAddr := trans.nodes[destination.NodeInstanceId]
	if nodeAddr == "" {
		cfgNode := trans.system.NodeConfig(destination.NodeInstanceId)
		if cfgNode == nil {
			trans.nodeMutex.Unlock()
			return nil, goactor.ErrorWrapf(goactor.ErrNodeMiss, "node:%d", destination.NodeInstanceId)
		}
		var ok bool
		nodeAddr, ok = cfgNode.Transports[trans.Name()]
		if !ok {
			trans.nodeMutex.Unlock()
			return nil, goactor.ErrorWrapf(goactor.ErrTransportMiss, "trans:%d", trans.Name())
		}

		trans.nodes[destination.NodeInstanceId] = nodeAddr
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
			goactor.BuildHeaderString(goactor.HeaderIdTransAddress, trans.topic),
		)

		trans.responses[transSession] = msg
		trans.responseMutex.Unlock()
	}

	buf, pberr := proto.Marshal(msg.ToPB())
	if pberr != nil {
		return nil, goactor.ErrorWrapf(pberr, "pb marshal error")
	}

	err := trans.nc.Publish(nodeAddr, buf)
	if err != nil {
		return nil, goactor.ErrorWrapf(err, "publish error %s", nodeAddr)
	}
	if reqsession != 0 {
		return func() {
			trans.responseMutex.Lock()
			delete(trans.responses, int32(transSession))
			trans.responseMutex.Unlock()
		}, nil
	}
	return nil, nil
}
