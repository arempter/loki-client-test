// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: logproto.proto

package logproto

import (
	fmt "fmt"
	_ "github.com/gogo/protobuf/gogoproto"
	github_com_gogo_protobuf_types "github.com/gogo/protobuf/types"
	proto "github.com/golang/protobuf/proto"
	_ "google.golang.org/protobuf/types/known/timestamppb"
	io "io"
	math "math"
	math_bits "math/bits"
	time "time"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf
var _ = time.Kitchen

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type PushRequest struct {
	Streams              []Stream `protobuf:"bytes,1,rep,name=streams,proto3,customtype=Stream" json:"streams"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *PushRequest) Reset()         { *m = PushRequest{} }
func (m *PushRequest) String() string { return proto.CompactTextString(m) }
func (*PushRequest) ProtoMessage()    {}
func (*PushRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_7a8976f235a02f79, []int{0}
}
func (m *PushRequest) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *PushRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_PushRequest.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *PushRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_PushRequest.Merge(m, src)
}
func (m *PushRequest) XXX_Size() int {
	return m.Size()
}
func (m *PushRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_PushRequest.DiscardUnknown(m)
}

var xxx_messageInfo_PushRequest proto.InternalMessageInfo

type StreamAdapter struct {
	Labels               string         `protobuf:"bytes,1,opt,name=labels,proto3" json:"labels"`
	Entries              []EntryAdapter `protobuf:"bytes,2,rep,name=entries,proto3" json:"entries"`
	XXX_NoUnkeyedLiteral struct{}       `json:"-"`
	XXX_unrecognized     []byte         `json:"-"`
	XXX_sizecache        int32          `json:"-"`
}

func (m *StreamAdapter) Reset()         { *m = StreamAdapter{} }
func (m *StreamAdapter) String() string { return proto.CompactTextString(m) }
func (*StreamAdapter) ProtoMessage()    {}
func (*StreamAdapter) Descriptor() ([]byte, []int) {
	return fileDescriptor_7a8976f235a02f79, []int{1}
}
func (m *StreamAdapter) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *StreamAdapter) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_StreamAdapter.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *StreamAdapter) XXX_Merge(src proto.Message) {
	xxx_messageInfo_StreamAdapter.Merge(m, src)
}
func (m *StreamAdapter) XXX_Size() int {
	return m.Size()
}
func (m *StreamAdapter) XXX_DiscardUnknown() {
	xxx_messageInfo_StreamAdapter.DiscardUnknown(m)
}

var xxx_messageInfo_StreamAdapter proto.InternalMessageInfo

func (m *StreamAdapter) GetLabels() string {
	if m != nil {
		return m.Labels
	}
	return ""
}

func (m *StreamAdapter) GetEntries() []EntryAdapter {
	if m != nil {
		return m.Entries
	}
	return nil
}

type EntryAdapter struct {
	Timestamp            time.Time `protobuf:"bytes,1,opt,name=timestamp,proto3,stdtime" json:"ts"`
	Line                 string    `protobuf:"bytes,2,opt,name=line,proto3" json:"line"`
	XXX_NoUnkeyedLiteral struct{}  `json:"-"`
	XXX_unrecognized     []byte    `json:"-"`
	XXX_sizecache        int32     `json:"-"`
}

func (m *EntryAdapter) Reset()         { *m = EntryAdapter{} }
func (m *EntryAdapter) String() string { return proto.CompactTextString(m) }
func (*EntryAdapter) ProtoMessage()    {}
func (*EntryAdapter) Descriptor() ([]byte, []int) {
	return fileDescriptor_7a8976f235a02f79, []int{2}
}
func (m *EntryAdapter) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *EntryAdapter) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_EntryAdapter.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *EntryAdapter) XXX_Merge(src proto.Message) {
	xxx_messageInfo_EntryAdapter.Merge(m, src)
}
func (m *EntryAdapter) XXX_Size() int {
	return m.Size()
}
func (m *EntryAdapter) XXX_DiscardUnknown() {
	xxx_messageInfo_EntryAdapter.DiscardUnknown(m)
}

var xxx_messageInfo_EntryAdapter proto.InternalMessageInfo

func (m *EntryAdapter) GetTimestamp() time.Time {
	if m != nil {
		return m.Timestamp
	}
	return time.Time{}
}

func (m *EntryAdapter) GetLine() string {
	if m != nil {
		return m.Line
	}
	return ""
}

func init() {
	proto.RegisterType((*PushRequest)(nil), "logproto.PushRequest")
	proto.RegisterType((*StreamAdapter)(nil), "logproto.StreamAdapter")
	proto.RegisterType((*EntryAdapter)(nil), "logproto.EntryAdapter")
}

func init() { proto.RegisterFile("logproto.proto", fileDescriptor_7a8976f235a02f79) }

var fileDescriptor_7a8976f235a02f79 = []byte{
	// 328 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x54, 0x50, 0xcd, 0x4a, 0xc3, 0x40,
	0x10, 0x6e, 0xaa, 0xf4, 0x67, 0xab, 0x15, 0x02, 0x6a, 0x29, 0x9a, 0x95, 0x9c, 0x7a, 0x69, 0x02,
	0xf5, 0x09, 0x1a, 0x10, 0x7a, 0x94, 0x28, 0x08, 0xde, 0x92, 0x3a, 0x6e, 0x43, 0x37, 0xd9, 0x98,
	0x9d, 0x14, 0x7c, 0x0b, 0x8f, 0x3e, 0x52, 0x8f, 0x9e, 0x3d, 0x44, 0xa9, 0xb7, 0x3e, 0x85, 0x74,
	0x93, 0x6d, 0xf4, 0x32, 0x33, 0xdf, 0xec, 0x37, 0xf3, 0xcd, 0x7e, 0xa4, 0xcf, 0x05, 0x4b, 0x33,
	0x81, 0xc2, 0x51, 0xd1, 0xec, 0x68, 0x3c, 0xa4, 0x4c, 0x08, 0xc6, 0xc1, 0x55, 0x28, 0xcc, 0x9f,
	0x5d, 0x8c, 0x62, 0x90, 0x18, 0xc4, 0x69, 0x49, 0x1d, 0x8e, 0x59, 0x84, 0x8b, 0x3c, 0x74, 0xe6,
	0x22, 0x76, 0x99, 0x60, 0xa2, 0x66, 0xee, 0x90, 0x02, 0xaa, 0x2a, 0xe9, 0xf6, 0x03, 0xe9, 0xdd,
	0xe6, 0x72, 0xe1, 0xc3, 0x4b, 0x0e, 0x12, 0xcd, 0x19, 0x69, 0x4b, 0xcc, 0x20, 0x88, 0xe5, 0xc0,
	0xb8, 0x3a, 0x18, 0xf5, 0x26, 0xe7, 0xce, 0xfe, 0x94, 0x3b, 0xf5, 0x30, 0x7d, 0x0a, 0x52, 0x84,
	0xcc, 0x3b, 0xfd, 0x2c, 0x68, 0xab, 0x6c, 0x6d, 0x0b, 0xaa, 0xa7, 0x7c, 0x5d, 0xd8, 0x2b, 0x72,
	0xfc, 0x6f, 0xc0, 0xb4, 0x49, 0x8b, 0x07, 0x21, 0xf0, 0xdd, 0x66, 0x63, 0xd4, 0xf5, 0xc8, 0xb6,
	0xa0, 0x55, 0xc7, 0xaf, 0xb2, 0x39, 0x25, 0x6d, 0x48, 0x30, 0x8b, 0x40, 0x0e, 0x9a, 0x4a, 0xfe,
	0xac, 0x96, 0xbf, 0x49, 0x30, 0x7b, 0xd5, 0xea, 0x27, 0xeb, 0x82, 0x36, 0x76, 0xba, 0x15, 0xdd,
	0xd7, 0x85, 0xbd, 0x22, 0x47, 0x7f, 0x99, 0xe6, 0x8c, 0x74, 0xf7, 0x16, 0x29, 0xe5, 0xde, 0x64,
	0xe8, 0x94, 0x26, 0x3a, 0xda, 0x1a, 0xe7, 0x5e, 0x33, 0xbc, 0x7e, 0xb5, 0xb8, 0x89, 0xf2, 0xed,
	0x8b, 0x1a, 0x7e, 0x3d, 0x6c, 0x5e, 0x90, 0x43, 0x1e, 0x25, 0x30, 0x68, 0xaa, 0xf3, 0x3b, 0xdb,
	0x82, 0x2a, 0xec, 0xab, 0xe8, 0xb9, 0xeb, 0x8d, 0x65, 0x7c, 0x6c, 0x2c, 0xe3, 0x7b, 0x63, 0x19,
	0xef, 0x3f, 0x56, 0xe3, 0xf1, 0x92, 0x8b, 0x65, 0x34, 0x9e, 0xf3, 0x08, 0x12, 0x1c, 0x23, 0x48,
	0x74, 0xd3, 0x25, 0x73, 0xf5, 0x7f, 0xc2, 0x96, 0x4a, 0xd7, 0xbf, 0x01, 0x00, 0x00, 0xff, 0xff,
	0x3c, 0xd2, 0xfe, 0xd3, 0xec, 0x01, 0x00, 0x00,
}

func (m *PushRequest) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *PushRequest) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *PushRequest) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.XXX_unrecognized != nil {
		i -= len(m.XXX_unrecognized)
		copy(dAtA[i:], m.XXX_unrecognized)
	}
	if len(m.Streams) > 0 {
		for iNdEx := len(m.Streams) - 1; iNdEx >= 0; iNdEx-- {
			{
				size := m.Streams[iNdEx].Size()
				i -= size
				if _, err := m.Streams[iNdEx].MarshalTo(dAtA[i:]); err != nil {
					return 0, err
				}
				i = encodeVarintLogproto(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0xa
		}
	}
	return len(dAtA) - i, nil
}

func (m *StreamAdapter) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *StreamAdapter) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *StreamAdapter) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.XXX_unrecognized != nil {
		i -= len(m.XXX_unrecognized)
		copy(dAtA[i:], m.XXX_unrecognized)
	}
	if len(m.Entries) > 0 {
		for iNdEx := len(m.Entries) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Entries[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintLogproto(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x12
		}
	}
	if len(m.Labels) > 0 {
		i -= len(m.Labels)
		copy(dAtA[i:], m.Labels)
		i = encodeVarintLogproto(dAtA, i, uint64(len(m.Labels)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *EntryAdapter) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *EntryAdapter) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *EntryAdapter) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.XXX_unrecognized != nil {
		i -= len(m.XXX_unrecognized)
		copy(dAtA[i:], m.XXX_unrecognized)
	}
	if len(m.Line) > 0 {
		i -= len(m.Line)
		copy(dAtA[i:], m.Line)
		i = encodeVarintLogproto(dAtA, i, uint64(len(m.Line)))
		i--
		dAtA[i] = 0x12
	}
	n1, err1 := github_com_gogo_protobuf_types.StdTimeMarshalTo(m.Timestamp, dAtA[i-github_com_gogo_protobuf_types.SizeOfStdTime(m.Timestamp):])
	if err1 != nil {
		return 0, err1
	}
	i -= n1
	i = encodeVarintLogproto(dAtA, i, uint64(n1))
	i--
	dAtA[i] = 0xa
	return len(dAtA) - i, nil
}

func encodeVarintLogproto(dAtA []byte, offset int, v uint64) int {
	offset -= sovLogproto(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *PushRequest) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	if len(m.Streams) > 0 {
		for _, e := range m.Streams {
			l = e.Size()
			n += 1 + l + sovLogproto(uint64(l))
		}
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *StreamAdapter) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Labels)
	if l > 0 {
		n += 1 + l + sovLogproto(uint64(l))
	}
	if len(m.Entries) > 0 {
		for _, e := range m.Entries {
			l = e.Size()
			n += 1 + l + sovLogproto(uint64(l))
		}
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func (m *EntryAdapter) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = github_com_gogo_protobuf_types.SizeOfStdTime(m.Timestamp)
	n += 1 + l + sovLogproto(uint64(l))
	l = len(m.Line)
	if l > 0 {
		n += 1 + l + sovLogproto(uint64(l))
	}
	if m.XXX_unrecognized != nil {
		n += len(m.XXX_unrecognized)
	}
	return n
}

func sovLogproto(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozLogproto(x uint64) (n int) {
	return sovLogproto(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *PushRequest) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowLogproto
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: PushRequest: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: PushRequest: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Streams", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLogproto
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthLogproto
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthLogproto
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Streams = append(m.Streams, Stream{})
			if err := m.Streams[len(m.Streams)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipLogproto(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthLogproto
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *StreamAdapter) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowLogproto
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: StreamAdapter: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: StreamAdapter: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Labels", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLogproto
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthLogproto
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthLogproto
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Labels = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Entries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLogproto
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthLogproto
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthLogproto
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Entries = append(m.Entries, EntryAdapter{})
			if err := m.Entries[len(m.Entries)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipLogproto(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthLogproto
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *EntryAdapter) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowLogproto
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: EntryAdapter: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: EntryAdapter: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Timestamp", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLogproto
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthLogproto
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthLogproto
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			if err := github_com_gogo_protobuf_types.StdTimeUnmarshal(&m.Timestamp, dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Line", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowLogproto
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthLogproto
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthLogproto
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Line = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipLogproto(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if (skippy < 0) || (iNdEx+skippy) < 0 {
				return ErrInvalidLengthLogproto
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			m.XXX_unrecognized = append(m.XXX_unrecognized, dAtA[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipLogproto(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowLogproto
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowLogproto
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowLogproto
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthLogproto
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupLogproto
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthLogproto
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthLogproto        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowLogproto          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupLogproto = fmt.Errorf("proto: unexpected end of group")
)
