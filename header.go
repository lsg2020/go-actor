package go_actor

type HeaderValType int

const (
	HeaderInt       HeaderValType = 1
	HeaderString    HeaderValType = 2
	HeaderBytes     HeaderValType = 3
	HeaderActorAddr HeaderValType = 4
	HeaderInterface HeaderValType = 5
)

const (
	HeaderIdMethod              = 1
	HeaderIdProtocol            = 2
	HeaderIdSession             = 3
	HeaderIdTransport           = 4
	HeaderIdSource              = 5
	HeaderIdDestination         = 6
	HeaderIdTransSession        = 7
	HeaderIdTransAddress        = 8
	HeaderIdRequestProtoPackCtx = 9
	HeaderIdTracingSpan         = 10
	HeaderIdTracingSpanCarrier  = 11
)

type Header struct {
	Id           int
	Type         HeaderValType
	Private      bool
	ValInt       int
	ValStr       string
	ValAddr      *ActorAddr
	ValBytes     []byte
	ValInterface interface{}
}

func BuildHeaderIntRaw(id int, val int, private bool) Header {
	return Header{Id: id, Type: HeaderInt, ValInt: val, Private: private}
}

func BuildHeaderInt(id int, val int) Header {
	return BuildHeaderIntRaw(id, val, false)
}

func BuildHeaderStringRaw(id int, val string, private bool) Header {
	return Header{Id: id, Type: HeaderString, ValStr: val, Private: private}
}

func BuildHeaderString(id int, val string) Header {
	return BuildHeaderStringRaw(id, val, false)
}

func BuildHeaderBytesRaw(id int, val []byte, private bool) Header {
	return Header{Id: id, Type: HeaderBytes, ValBytes: val, Private: private}
}

func BuildHeaderBytes(id int, val []byte) Header {
	return BuildHeaderBytesRaw(id, val, false)
}

func BuildHeaderActorRaw(id int, val *ActorAddr, private bool) Header {
	return Header{Id: id, Type: HeaderActorAddr, ValAddr: val, Private: private}
}

func BuildHeaderActor(id int, val *ActorAddr) Header {
	return BuildHeaderActorRaw(id, val, false)
}

func BuildHeaderInterfaceRaw(id int, val interface{}, private bool) Header {
	return Header{Id: id, Type: HeaderInterface, ValInterface: val, Private: private}
}

func BuildHeaderInterface(id int, val interface{}) Header {
	return BuildHeaderInterfaceRaw(id, val, false)
}

type Headers []Header

func (h Headers) Put(datas ...Header) Headers {
	if cap(h) == 0 {
		h = make([]Header, 0, len(datas))
	}
	for _, data := range datas {
		exists := h.Get(data.Id)
		if exists != nil {
			*exists = data
		} else {
			h = append(h, data)
		}
	}
	return h
}

func (h Headers) Get(id int) *Header {
	for i := 0; i < len(h); i++ {
		if h[i].Id == id {
			return &h[i]
		}
	}
	return nil
}

func (h Headers) GetInt(id int) int {
	header := h.Get(id)
	if header != nil {
		return header.ValInt
	}
	return 0
}

func (h Headers) GetStr(id int) string {
	header := h.Get(id)
	if header != nil {
		return header.ValStr
	}
	return ""
}

func (h Headers) GetBytes(id int) []byte {
	header := h.Get(id)
	if header != nil {
		return header.ValBytes
	}
	return nil
}

func (h Headers) GetAddr(id int) *ActorAddr {
	header := h.Get(id)
	if header != nil {
		return header.ValAddr
	}
	return nil
}

func (h Headers) GetInterface(id int) interface{} {
	header := h.Get(id)
	if header != nil {
		return header.ValInterface
	}
	return nil
}

type HeadersWrap struct {
	headers Headers
}

func (hw *HeadersWrap) Cap(amount int) *HeadersWrap {
	if cap(hw.headers) == 0 {
		hw.headers = make([]Header, 0, amount)
		return hw
	}
	return hw
}

func (hw *HeadersWrap) Put(datas ...Header) *HeadersWrap {
	hw.headers = hw.headers.Put(datas...)
	return hw
}

func (hw *HeadersWrap) Get(id int) *Header {
	return hw.headers.Get(id)
}

func (hw *HeadersWrap) GetInt(id int) int {
	return hw.headers.GetInt(id)
}

func (hw *HeadersWrap) GetStr(id int) string {
	return hw.headers.GetStr(id)
}

func (hw *HeadersWrap) GetBytes(id int) []byte {
	return hw.headers.GetBytes(id)
}

func (hw *HeadersWrap) GetAddr(id int) *ActorAddr {
	return hw.headers.GetAddr(id)
}

func (hw *HeadersWrap) GetInterface(id int) interface{} {
	return hw.headers.GetInterface(id)
}
