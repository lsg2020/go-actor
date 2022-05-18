package nats

import (
	"fmt"
	"sync"

	goactor "github.com/lsg2020/go-actor"
	message "github.com/lsg2020/go-actor/pb"
	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
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
		responseMsg := &goactor.DispatchMessage{
			ResponseErr: err,
			Content:     data,
		}
		responseMsg.Headers.Put(
			goactor.BuildHeaderInt(goactor.HeaderIdProtocol, goactor.ProtocolResponse),
			goactor.BuildHeaderInt(goactor.HeaderIdSession, msg.Headers.GetInt(goactor.HeaderIdSession)),
			goactor.BuildHeaderInt(goactor.HeaderIdTransSession, msg.Headers.GetInt(goactor.HeaderIdTransSession)),
		)
		buf, perr := proto.Marshal(responseMsg.ToPB())
		if perr != nil {
			system.Logger().Errorf("response pack error %s\n", perr.Error())
			return
		}
		perr = nc.Publish(msg.Headers.GetStr(goactor.HeaderIdTransAddress), buf)
		if err != nil {
			system.Logger().Errorf("response pack error %s\n", perr.Error())
		}
	}

	trans.nc = nc
	trans.topic = "node_" + node
	_, err = nc.Subscribe(trans.topic, func(m *nats.Msg) {
		msgBuf := m.Data

		msg := &message.Message{}
		err = proto.Unmarshal(msgBuf, msg)
		if err != nil {
			system.Logger().Warnf("receive invalid pack %s\n", err.Error())
			return
		}

		dispatch := &goactor.DispatchMessage{
			DispatchResponse: response,
		}
		dispatch.FromPB(msg)

		if dispatch.Headers.GetInt(goactor.HeaderIdProtocol) == goactor.ProtocolResponse {
			var requestMsg *goactor.DispatchMessage
			trans.responseMutex.Lock()
			requestMsg = trans.responses[int32(dispatch.Headers.GetInt(goactor.HeaderIdTransSession))]
			trans.responseMutex.Unlock()
			if requestMsg != nil {
				requestMsg.DispatchResponse(dispatch, dispatch.ResponseErr, dispatch.Content)
			}
		} else {
			err := trans.system.Dispatch(dispatch)
			if err != nil {
				system.Logger().Warnf("dispatch message error %s\n", err.Error())
			}
		}
	})

	return err
}

func (trans *NatsTransport) Send(msg *goactor.DispatchMessage) (goactor.SessionCancel, error) {
	destination := msg.Headers.GetAddr(goactor.HeaderIdDestination)
	if destination == nil {
		return nil, goactor.ErrNeedDestination
	}

	trans.nodeMutex.Lock()
	nodeAddr := trans.nodes[destination.NodeInstanceId]
	if nodeAddr == "" {
		cfgNode := trans.system.NodeConfig(destination.NodeInstanceId)
		if cfgNode == nil {
			trans.nodeMutex.Unlock()
			return nil, goactor.Errorf("node not exists:%d", destination.NodeInstanceId)
		}
		var ok bool
		nodeAddr, ok = cfgNode.Transports[trans.Name()]
		if !ok {
			trans.nodeMutex.Unlock()
			return nil, goactor.Errorf("node not exists:%d", destination.NodeInstanceId)
		}

		trans.nodes[destination.NodeInstanceId] = nodeAddr
	}
	trans.nodeMutex.Unlock()

	reqsession := msg.Headers.GetInt(goactor.HeaderIdSession)
	transSession := int32(0)
	if reqsession >= 0 {
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
		return nil, goactor.ErrorWrap(pberr)
	}

	err := trans.nc.Publish(nodeAddr, buf)
	if err != nil {
		return nil, goactor.ErrorWrap(err)
	}
	if reqsession >= 0 {
		return func() {
			trans.responseMutex.Lock()
			delete(trans.responses, int32(transSession))
			trans.responseMutex.Unlock()
		}, nil
	}
	return nil, nil
}
