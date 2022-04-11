package nats

import (
	"fmt"
	"sync"

	"github.com/lsg2020/gactor"
	message "github.com/lsg2020/gactor/pb"
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
	system  *gactor.ActorSystem
	servers string
	topic   string

	nc *nats.Conn

	nodeMutex sync.Mutex
	nodes     map[uint64]string

	responseMux sync.Mutex
	responses   map[int32]*gactor.DispatchMessage
	session     int32
}

func (trans *NatsTransport) Name() string {
	return "nats"
}

func (trans *NatsTransport) URI() string {
	return trans.topic
}

func (trans *NatsTransport) Init(system *gactor.ActorSystem) error {
	trans.nodes = make(map[uint64]string)
	trans.responses = make(map[int32]*gactor.DispatchMessage)
	trans.session = 1
	trans.system = system
	nc, err := nats.Connect(trans.servers)
	if err != nil {
		return err
	}

	node := fmt.Sprintf("%d", system.InstanceID())

	response := func(msg *gactor.DispatchMessage, err *gactor.ActorError, data interface{}) {
		responseMsg := &gactor.DispatchMessage{
			ResponseErr: err,
			Content:     data,
		}
		responseMsg.Headers.Put(
			gactor.BuildHeaderInt(gactor.HeaderIdProtocol, gactor.ProtocolResponse),
			gactor.BuildHeaderInt(gactor.HeaderIdSession, msg.Headers.GetInt(gactor.HeaderIdSession)),
			gactor.BuildHeaderInt(gactor.HeaderIdTransSession, msg.Headers.GetInt(gactor.HeaderIdTransSession)),
		)
		buf, perr := proto.Marshal(responseMsg.ToPB())
		if perr != nil {
			system.Logger().Errorf("response pack error %s\n", perr.Error())
			return
		}
		perr = nc.Publish(msg.Headers.GetStr(gactor.HeaderIdTransAddress), buf)
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

		dispatch := &gactor.DispatchMessage{
			DispatchResponse: response,
		}
		dispatch.FromPB(msg)

		if dispatch.Headers.GetInt(gactor.HeaderIdProtocol) == gactor.ProtocolResponse {
			var requestMsg *gactor.DispatchMessage
			trans.responseMux.Lock()
			requestMsg = trans.responses[int32(dispatch.Headers.GetInt(gactor.HeaderIdTransSession))]
			trans.responseMux.Unlock()
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

func (trans *NatsTransport) Send(msg *gactor.DispatchMessage) (gactor.SessionCancel, *gactor.ActorError) {
	destination := msg.Headers.GetAddr(gactor.HeaderIdDestination)
	if destination == nil {
		return nil, gactor.ErrNeedDestination
	}

	trans.nodeMutex.Lock()
	nodeAddr := trans.nodes[destination.NodeInstanceId]
	if nodeAddr == "" {
		cfgNode := trans.system.NodeConfig(destination.NodeInstanceId)
		if cfgNode == nil {
			trans.nodeMutex.Unlock()
			return nil, gactor.Errorf("node not exists:%d", destination.NodeInstanceId)
		}
		var ok bool
		nodeAddr, ok = cfgNode.Transports[trans.Name()]
		if !ok {
			trans.nodeMutex.Unlock()
			return nil, gactor.Errorf("node not exists:%d", destination.NodeInstanceId)
		}

		trans.nodes[destination.NodeInstanceId] = nodeAddr
	}
	trans.nodeMutex.Unlock()

	reqsession := msg.Headers.GetInt(gactor.HeaderIdSession)
	transSession := int32(0)
	if reqsession >= 0 {
		trans.responseMux.Lock()
		trans.session++
		transSession = trans.session
		msg.Headers.Put(
			gactor.BuildHeaderInt(gactor.HeaderIdTransSession, int(transSession)),
			gactor.BuildHeaderString(gactor.HeaderIdTransAddress, trans.topic),
		)

		trans.responses[transSession] = msg
		trans.responseMux.Unlock()
	}

	buf, pberr := proto.Marshal(msg.ToPB())
	if pberr != nil {
		return nil, gactor.ErrorWrap(pberr)
	}

	err := trans.nc.Publish(nodeAddr, buf)
	if err != nil {
		return nil, gactor.ErrorWrap(err)
	}
	if reqsession >= 0 {
		return func() {
			trans.responseMux.Lock()
			delete(trans.responses, int32(transSession))
			trans.responseMux.Unlock()
		}, nil
	}
	return nil, nil
}
