package tcp

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"sync"
	"time"

	goactor "github.com/lsg2020/go-actor"
	message "github.com/lsg2020/go-actor/pb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// NewTcp 创建一个tcp传输器
func NewTcp(listenAddr string, publicAddr string) *TcpTransport {
	trans := &TcpTransport{
		listenAddr: listenAddr,
		publicAddr: publicAddr,
		connects:   make(map[uint64]*tcpConnect),
		responses:  make(map[int32]*goactor.DispatchMessage),
	}
	return trans
}

type tcpConnect struct {
	conn     net.Conn
	sendCond *sync.Cond
	sendList [][]byte
	onClose  func()
}

func (node *tcpConnect) send(buf []byte) {
	node.sendCond.L.Lock()
	node.sendList = append(node.sendList, buf)
	node.sendCond.Signal()
	node.sendCond.L.Unlock()
}

type TcpTransport struct {
	listenAddr string
	publicAddr string
	system     *goactor.ActorSystem

	nodeMutex sync.Mutex
	connects  map[uint64]*tcpConnect

	responseMutex sync.Mutex
	responses     map[int32]*goactor.DispatchMessage
	session       int32
}

func (trans *TcpTransport) Name() string {
	return "tcp"
}

func (trans *TcpTransport) URI() string {
	return trans.publicAddr
}

func (trans *TcpTransport) newTcpConnect(conn net.Conn, onclose func()) *tcpConnect {
	node := &tcpConnect{
		conn:     conn,
		sendList: make([][]byte, 0, 1024),
		sendCond: sync.NewCond(new(sync.Mutex)),
		onClose:  onclose,
	}
	return node
}

func (trans *TcpTransport) Init(system *goactor.ActorSystem) error {
	trans.system = system

	listener, err := net.Listen("tcp", trans.listenAddr)
	if err != nil {
		return err
	}
	go func() {
		defer listener.Close()
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}

			tcpConn := trans.newTcpConnect(conn, nil)
			go trans.reader(tcpConn)
			go trans.sender(tcpConn)
		}
	}()
	return nil
}

func (trans *TcpTransport) Send(msg *goactor.DispatchMessage) (goactor.SessionCancel, error) {
	reqsession := msg.SessionId
	destination := msg.Destination
	if destination == nil {
		return nil, goactor.ErrNeedDestination
	}

	trans.nodeMutex.Lock()
	tcpConn := trans.connects[destination.NodeInstanceId]
	if tcpConn == nil {
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

		conn, err := net.Dial("tcp", nodeAddr)
		if err != nil {
			trans.nodeMutex.Unlock()
			return nil, goactor.ErrorWrapf(err, "connect err %s", nodeAddr)
		}

		tcpConn = trans.newTcpConnect(conn, func() {
			trans.nodeMutex.Lock()
			defer trans.nodeMutex.Unlock()
			delete(trans.connects, destination.NodeInstanceId)
		})
		trans.connects[destination.NodeInstanceId] = tcpConn

		go trans.reader(tcpConn)
		go trans.sender(tcpConn)
	}
	trans.nodeMutex.Unlock()

	transSession := int32(0)
	if reqsession != 0 {
		trans.responseMutex.Lock()
		trans.session++
		transSession = trans.session
		msg.Headers.Put(goactor.BuildHeaderInt(goactor.HeaderIdTransSession, int(transSession)))

		trans.responses[transSession] = msg
		trans.responseMutex.Unlock()
	}

	buf, pberr := proto.Marshal(msg.ToPB())
	if pberr != nil {
		return nil, goactor.ErrorWrapf(pberr, "pb marshal")
	}
	tcpConn.send(buf)

	if reqsession != 0 {
		return func() {
			trans.responseMutex.Lock()
			delete(trans.responses, transSession)
			trans.responseMutex.Unlock()
		}, nil
	}
	return nil, nil
}

func (trans *TcpTransport) reader(conn *tcpConnect) {
	bufReader := bufio.NewReader(conn.conn)
	defer func() {
		conn.conn.Close()
		if conn.onClose != nil {
			conn.onClose()
		}
	}()

	response := func(msg *goactor.DispatchMessage, err error, data interface{}) {
		headers := goactor.Headers{
			goactor.BuildHeaderInt(goactor.HeaderIdProtocol, goactor.ProtocolResponse),
			goactor.BuildHeaderInt(goactor.HeaderIdSession, msg.SessionId),
			goactor.BuildHeaderInt(goactor.HeaderIdTransSession, msg.Headers.GetInt(goactor.HeaderIdTransSession)),
		}
		responseMsg := goactor.NewDispatchMessage(nil, nil, nil, nil, nil, goactor.ProtocolResponse, msg.SessionId, headers, data, err, nil)

		buf, pberr := proto.Marshal(responseMsg.ToPB())
		if pberr != nil {
			return
		}
		conn.send(buf)
	}

	processMsg := func(msgBuf []byte) {
		msg := &message.Message{}
		err := proto.Unmarshal(msgBuf, msg)
		if err != nil {
			trans.system.Logger().Error("receive invalid pack", zap.Error(err))
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
			} else {
				trans.system.Logger().Error("dispatch response message not exists", zap.Int("session", dispatch.Headers.GetInt(goactor.HeaderIdTransSession)))
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

				trans.system.Logger().Error("dispatch message error", zap.Error(err))
			}
		}
	}

	buffer := make([]byte, 512*1024)
	bufferLen := 0

	for {
		n, err := bufReader.Read(buffer[bufferLen:])
		if err != nil {
			break
		}
		bufferLen += n

		bufIndex := 0
		for bufIndex+4 <= bufferLen {
			msgLen := int(binary.BigEndian.Uint32(buffer[bufIndex : bufIndex+4]))
			if 4+msgLen > len(buffer) {
				msgBuf := make([]byte, msgLen)
				bufLen := bufferLen - bufIndex - 4
				copy(msgBuf[:bufLen], buffer[bufIndex+4:bufferLen])
				n, err := io.ReadFull(bufReader, msgBuf[bufLen:])
				if n != (msgLen-bufLen) || err != nil {
					return
				}

				processMsg(msgBuf)
				bufIndex = 0
				bufferLen = 0
				break
			}
			if bufIndex+4+msgLen > bufferLen {
				break
			}

			bufIndex += (4 + msgLen)
			processMsg(buffer[bufIndex-msgLen : bufIndex])
		}

		if bufIndex > 0 {
			if bufIndex < bufferLen {
				copyLen := bufferLen - bufIndex
				copy(buffer[0:copyLen], buffer[bufferLen-copyLen:bufferLen])
			}
			bufferLen = 0
		}
	}
}

func (trans *TcpTransport) sender(conn *tcpConnect) {
	defer func() {
		conn.conn.Close()
		if conn.onClose != nil {
			conn.onClose()
		}
	}()

	buffer := make([]byte, 512*1024)
	bufferLen := 0

	workList := make([][]byte, 0, 1024)
	sizeBuf := make([]byte, 4)
	for {
		conn.sendCond.L.Lock()
		workList, conn.sendList = conn.sendList, workList
		if len(workList) == 0 {
			conn.sendCond.Wait()
			workList, conn.sendList = conn.sendList, workList
		}
		conn.sendCond.L.Unlock()

		if len(workList) == 0 {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		for _, buf := range workList {
			if len(buf)+4 >= len(buffer) {
				if bufferLen > 0 {
					n, err := conn.conn.Write(buffer[:bufferLen])
					if n != bufferLen || err != nil {
						return
					}
					bufferLen = 0
				}

				binary.BigEndian.PutUint32(sizeBuf, uint32(len(buf)))
				n, err := conn.conn.Write(sizeBuf)
				if n != len(sizeBuf) || err != nil {
					return
				}
				n, err = conn.conn.Write(buf)
				if n != len(buf) || err != nil {
					return
				}
				continue
			}

			if bufferLen+len(buf)+4 >= len(buffer) {
				n, err := conn.conn.Write(buffer[:bufferLen])
				if n != bufferLen || err != nil {
					return
				}
				bufferLen = 0
			}

			binary.BigEndian.PutUint32(buffer[bufferLen:bufferLen+4], uint32(len(buf)))
			bufferLen += 4
			copy(buffer[bufferLen:bufferLen+len(buf)], buf)
			bufferLen += len(buf)
		}

		if bufferLen > 0 {
			n, err := conn.conn.Write(buffer[:bufferLen])
			if n != bufferLen || err != nil {
				return
			}
		}

		bufferLen = 0
		workList = workList[:0]
	}
}
