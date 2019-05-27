package flv

import (
	"bytes"
	"compress/gzip"
	"github.com/dinalt/joy4/av"
	"github.com/dinalt/joy4/format/flv/flvio"
	"io"
	//	"log"
	"sync"
)

type CompressQueue struct {
	tags    [][]byte
	header  []byte
	bufw    *bytes.Buffer
	b       []byte
	cond    *sync.Cond
	lock    *sync.RWMutex
	cmpr    *gzip.Writer
	streams []av.CodecData
}

func NewCompressQueue() *CompressQueue {

	res := &CompressQueue{
		tags: make([][]byte, 0, 1024),
		b:    make([]byte, 256),
		bufw: &bytes.Buffer{},
		lock: &sync.RWMutex{},
	}
	res.cmpr = gzip.NewWriter(res.bufw)
	res.cond = sync.NewCond(res.lock.RLocker())

	return res

}

func (self *CompressQueue) WriteHeader(streams []av.CodecData) (err error) {

	self.lock.Lock()
	defer self.lock.Unlock()

	var flags uint8
	for _, stream := range streams {
		if stream.Type().IsVideo() {
			flags |= flvio.FILE_HAS_VIDEO
		} else if stream.Type().IsAudio() {
			flags |= flvio.FILE_HAS_AUDIO
		}
	}

	self.bufw.Reset()
	self.cmpr.Reset(self.bufw)

	n := flvio.FillFileHeader(self.b, flags)
	if _, err = self.cmpr.Write(self.b[:n]); err != nil {
		return
	}

	for _, stream := range streams {
		var tag flvio.Tag
		var ok bool
		if tag, ok, err = CodecDataToTag(stream); err != nil {
			return
		}
		if ok {
			if err = flvio.WriteTag(self.cmpr, tag, 0, self.b); err != nil {
				return
			}
		}
	}
	buf := make([]byte, self.bufw.Len())
	copy(buf, self.bufw.Bytes())
	self.header = buf
	self.streams = streams
	return nil

}

func (self *CompressQueue) WritePacket(pkt av.Packet) (err error) {

	self.lock.Lock()
	defer self.lock.Unlock()

	stream := self.streams[pkt.Idx]
	tag, timestamp := PacketToTag(pkt, stream)
	self.bufw.Reset()
	self.cmpr.Reset(self.bufw)

	if err = flvio.WriteTag(self.cmpr, tag, timestamp, self.b); err != nil {
		return
	}

	buf := make([]byte, self.bufw.Len())
	copy(buf, self.bufw.Bytes())

	self.tags = append(self.tags, buf)
	self.cond.Broadcast()
	return

}

func (self *CompressQueue) WriteTrailer() (err error) {
	return
}

func (self *CompressQueue) First() io.Reader {

	return self.newCursor(0)

}

func (self *CompressQueue) Last() io.Reader {

	return self.newCursor(len(self.tags) - 1)

}

func (self *CompressQueue) newCursor(p int) *compressQueueCursor {

	return &compressQueueCursor{
		q:   self,
		pos: p,
	}

}

type compressQueueCursor struct {
	q          *CompressQueue
	pos        int
	tagPos     int
	headerSent bool
}

func (c *compressQueueCursor) Read(p []byte) (n int, err error) {

	c.q.cond.L.Lock()
	defer c.q.cond.L.Unlock()

	var tagEnd bool

	tag := c.getTag()
	n = len(tag) - c.tagPos

	if n > len(p) {
		n = len(p)
	} else {
		tagEnd = true
	}

	copy(p, tag[c.tagPos:c.tagPos+n])

	if tagEnd {
		c.tagPos = 0
		if !c.headerSent {
			c.headerSent = true
		} else {
			c.pos++
		}
	} else {
		c.tagPos += n
	}

	return

}

func (c *compressQueueCursor) getTag() (tag []byte) {

	if !c.headerSent {
		tag = c.q.header
		return
	}

	for {

		if c.pos < len(c.q.tags) {
			tag = c.q.tags[c.pos]

			break
		}

		c.q.cond.Wait()

	}

	return

}
