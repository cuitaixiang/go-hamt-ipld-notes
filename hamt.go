package hamt

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	cid "github.com/ipfs/go-cid"
	cbor "github.com/ipfs/go-ipld-cbor"
	cbg "github.com/whyrusleeping/cbor-gen"
	xerrors "golang.org/x/xerrors"
)

const arrayWidth = 3
const defaultBitWidth = 8

type Node struct {
	// bit stamp
	Bitfield *big.Int   `refmt:"bf"`
	Pointers []*Pointer `refmt:"p"`

	bitWidth int

	// for fetching and storing children
	store cbor.IpldStore
}

// Option is a function that configures the node
type Option func(*Node)

// UseTreeBitWidth allows you to set the width of the HAMT tree
// in bits (from 1-8) via a customized hash function
// 定制hamt树的bit宽度，只能在[1，8]范围
func UseTreeBitWidth(bitWidth int) Option {
	return func(nd *Node) {
		if bitWidth > 0 && bitWidth <= 8 {
			nd.bitWidth = bitWidth
		}
	}
}

// NewNode creates a new IPLD HAMT Node with the given store and given
// options
// 创建node
func NewNode(cs cbor.IpldStore, options ...Option) *Node {
	nd := &Node{
		Bitfield: big.NewInt(0),
		Pointers: make([]*Pointer, 0),
		store:    cs,
		bitWidth: defaultBitWidth,
	}
	// apply functional options to node before using
	for _, option := range options {
		option(nd)
	}
	return nd
}

// key-value结构，value实现了un/MarshalCBOR功能
type KV struct {
	Key   string
	Value *cbg.Deferred
}

// 指针，包含KV/link/缓存
type Pointer struct {
	KVs  []*KV   `refmt:"v,omitempty"`
	Link cid.Cid `refmt:"l,omitempty"`

	// cached node to avoid too many serialization operations
	cache *Node
}

// 查找k对应的value
func (n *Node) Find(ctx context.Context, k string, out interface{}) error {
	return n.getValue(ctx, &hashBits{b: hash(k)}, k, func(kv *KV) error {
		// used to just see if the thing exists in the set
		if out == nil {
			return nil
		}

		if um, ok := out.(cbg.CBORUnmarshaler); ok {
			return um.UnmarshalCBOR(bytes.NewReader(kv.Value.Raw))
		}

		// 从kv中获取value
		if err := cbor.DecodeInto(kv.Value.Raw, out); err != nil {
			xerrors.Errorf("cbor decoding value: %w", err)
		}

		return nil
	})
}

func (n *Node) FindRaw(ctx context.Context, k string) ([]byte, error) {
	var ret []byte
	err := n.getValue(ctx, &hashBits{b: hash(k)}, k, func(kv *KV) error {
		ret = kv.Value.Raw
		return nil
	})
	return ret, err
}

func (n *Node) Delete(ctx context.Context, k string) error {
	return n.modifyValue(ctx, &hashBits{b: hash(k)}, k, nil)
}

var ErrNotFound = fmt.Errorf("not found")
var ErrMaxDepth = fmt.Errorf("attempted to traverse hamt beyond max depth")

// 获取值
func (n *Node) getValue(ctx context.Context, hv *hashBits, k string, cb func(*KV) error) error {
	idx, err := hv.Next(n.bitWidth)
	if err != nil {
		return ErrMaxDepth
	}

	// bit位为零，则没有
	if n.Bitfield.Bit(idx) == 0 {
		return ErrNotFound
	}

	cindex := byte(n.indexForBitPos(idx))

	c := n.getChild(cindex)
	// 如果还有孩子
	if c.isShard() {
		// 加载孩子节点
		chnd, err := c.loadChild(ctx, n.store, n.bitWidth)
		if err != nil {
			return err
		}

		// 在孩子节点查找
		return chnd.getValue(ctx, hv, k, cb)
	}

	// 没有孩子，则在kv中查找
	for _, kv := range c.KVs {
		if kv.Key == k {
			return cb(kv)
		}
	}

	return ErrNotFound
}

func (p *Pointer) loadChild(ctx context.Context, ns cbor.IpldStore, bitWidth int) (*Node, error) {
	if p.cache != nil {
		return p.cache, nil
	}

	out, err := LoadNode(ctx, ns, p.Link)
	if err != nil {
		return nil, err
	}
	out.bitWidth = bitWidth

	p.cache = out
	return out, nil
}

func LoadNode(ctx context.Context, cs cbor.IpldStore, c cid.Cid, options ...Option) (*Node, error) {
	var out Node
	if err := cs.Get(ctx, c, &out); err != nil {
		return nil, err
	}

	out.store = cs
	out.bitWidth = defaultBitWidth
	// apply functional options to node before using
	for _, option := range options {
		option(&out)
	}

	return &out, nil
}

func (n *Node) checkSize(ctx context.Context) (uint64, error) {
	c, err := n.store.Put(ctx, n)
	if err != nil {
		return 0, err
	}

	var def cbg.Deferred
	if err := n.store.Get(ctx, c, &def); err != nil {
		return 0, nil
	}

	totsize := uint64(len(def.Raw))
	for _, ch := range n.Pointers {
		if ch.isShard() {
			chnd, err := ch.loadChild(ctx, n.store, n.bitWidth)
			if err != nil {
				return 0, err
			}
			chsize, err := chnd.checkSize(ctx)
			if err != nil {
				return 0, err
			}
			totsize += chsize
		}
	}

	return totsize, nil
}

func (n *Node) Flush(ctx context.Context) error {
	for _, p := range n.Pointers {
		if p.cache != nil { // 清缓存
			if err := p.cache.Flush(ctx); err != nil {
				return err
			}

			c, err := n.store.Put(ctx, p.cache)
			if err != nil {
				return err
			}

			// 缓存置空
			p.cache = nil
			// 赋值link
			p.Link = c
		}
	}
	return nil
}

// SetRaw sets key k to cbor bytes raw
func (n *Node) SetRaw(ctx context.Context, k string, raw []byte) error {
	d := &cbg.Deferred{Raw: raw}
	return n.modifyValue(ctx, &hashBits{b: hash(k)}, k, d)
}

func (n *Node) Set(ctx context.Context, k string, v interface{}) error {
	var d *cbg.Deferred

	// 转为cbor原始字节
	cm, ok := v.(cbg.CBORMarshaler)
	if ok {
		buf := new(bytes.Buffer)
		if err := cm.MarshalCBOR(buf); err != nil {
			return err
		}
		d = &cbg.Deferred{Raw: buf.Bytes()}
	} else {
		b, err := cbor.DumpObject(v)
		if err != nil {
			return err
		}
		d = &cbg.Deferred{Raw: b}
	}

	return n.modifyValue(ctx, &hashBits{b: hash(k)}, k, d)
}

func (n *Node) cleanChild(chnd *Node, cindex byte) error {
	l := len(chnd.Pointers)
	switch {
	case l == 0:
		return fmt.Errorf("incorrectly formed HAMT")
	case l == 1:
		// TODO: only do this if its a value, cant do this for shards unless pairs requirements are met.

		ps := chnd.Pointers[0]
		if ps.isShard() {
			return nil
		}

		return n.setChild(cindex, ps)
	case l <= arrayWidth:
		var chvals []*KV
		for _, p := range chnd.Pointers {
			if p.isShard() {
				return nil
			}

			for _, sp := range p.KVs {
				if len(chvals) == arrayWidth {
					return nil
				}
				chvals = append(chvals, sp)
			}
		}
		return n.setChild(cindex, &Pointer{KVs: chvals})
	default:
		return nil
	}
}

func (n *Node) modifyValue(ctx context.Context, hv *hashBits, k string, v *cbg.Deferred) error {
	// 取指定字节宽度的索引整数
	idx, err := hv.Next(n.bitWidth)
	if err != nil {
		return ErrMaxDepth
	}

	// 查询idx位置上比特位是否为1
	if n.Bitfield.Bit(idx) != 1 { // 不为1，直接插入
		return n.insertChild(idx, k, v)
	}

	// 查询修改的位置
	cindex := byte(n.indexForBitPos(idx))

	child := n.getChild(cindex)
	if child.isShard() {
		chnd, err := child.loadChild(ctx, n.store, n.bitWidth)
		if err != nil {
			return err
		}

		if err := chnd.modifyValue(ctx, hv, k, v); err != nil {
			return err
		}

		// CHAMP optimization, ensure trees look correct after deletions
		if v == nil {
			if err := n.cleanChild(chnd, cindex); err != nil {
				return err
			}
		}

		return nil
	}

	if v == nil {
		for i, p := range child.KVs {
			if p.Key == k {
				if len(child.KVs) == 1 {
					return n.rmChild(cindex, idx)
				}

				copy(child.KVs[i:], child.KVs[i+1:])
				child.KVs = child.KVs[:len(child.KVs)-1]
				return nil
			}
		}
		return ErrNotFound
	}

	// check if key already exists
	for _, p := range child.KVs {
		if p.Key == k {
			p.Value = v
			return nil
		}
	}

	// If the array is full, create a subshard and insert everything into it
	if len(child.KVs) >= arrayWidth {
		sub := NewNode(n.store)
		sub.bitWidth = n.bitWidth
		hvcopy := &hashBits{b: hv.b, consumed: hv.consumed}
		if err := sub.modifyValue(ctx, hvcopy, k, v); err != nil {
			return err
		}

		for _, p := range child.KVs {
			chhv := &hashBits{b: hash(p.Key), consumed: hv.consumed}
			if err := sub.modifyValue(ctx, chhv, p.Key, p.Value); err != nil {
				return err
			}
		}

		c, err := n.store.Put(ctx, sub)
		if err != nil {
			return err
		}

		return n.setChild(cindex, &Pointer{Link: c})
	}

	// otherwise insert the new element into the array in order
	np := &KV{Key: k, Value: v}
	for i := 0; i < len(child.KVs); i++ {
		if k < child.KVs[i].Key {
			child.KVs = append(child.KVs[:i], append([]*KV{np}, child.KVs[i:]...)...)
			return nil
		}
	}
	child.KVs = append(child.KVs, np)
	return nil
}

// 插入孩子节点
func (n *Node) insertChild(idx int, k string, v *cbg.Deferred) error {
	if v == nil {
		return ErrNotFound
	}

	// 查询插入的位置
	i := n.indexForBitPos(idx)
	// 设置相应bit为1
	n.Bitfield.SetBit(n.Bitfield, idx, 1)

	// 构造pointer
	p := &Pointer{KVs: []*KV{{Key: k, Value: v}}}

	// 插入到i和i+1中间
	n.Pointers = append(n.Pointers[:i], append([]*Pointer{p}, n.Pointers[i:]...)...)
	return nil
}

// 设置第i个位置的指针
func (n *Node) setChild(i byte, p *Pointer) error {
	n.Pointers[i] = p
	return nil
}

// 移除孩子
func (n *Node) rmChild(i byte, idx int) error {
	// 前移覆盖
	copy(n.Pointers[i:], n.Pointers[i+1:])
	// 取n-1部分
	n.Pointers = n.Pointers[:len(n.Pointers)-1]
	// bit位置零
	n.Bitfield.SetBit(n.Bitfield, idx, 0)

	return nil
}

// 获取孩子
func (n *Node) getChild(i byte) *Pointer {
	if int(i) >= len(n.Pointers) || i < 0 {
		return nil
	}

	return n.Pointers[i]
}

// 深拷贝一个节点
func (n *Node) Copy() *Node {
	nn := NewNode(n.store)
	nn.bitWidth = n.bitWidth
	nn.Bitfield.Set(n.Bitfield)
	nn.Pointers = make([]*Pointer, len(n.Pointers))

	for i, p := range n.Pointers {
		pp := &Pointer{}
		if p.cache != nil {
			pp.cache = p.cache.Copy()
		}
		pp.Link = p.Link
		if p.KVs != nil {
			pp.KVs = make([]*KV, len(p.KVs))
			for j, kv := range p.KVs {
				pp.KVs[j] = &KV{Key: kv.Key, Value: kv.Value}
			}
		}
		nn.Pointers[i] = pp
	}

	return nn
}

// 是否计算了cid
func (p *Pointer) isShard() bool {
	return p.Link.Defined()
}

func (n *Node) ForEach(ctx context.Context, f func(k string, val interface{}) error) error {
	for _, p := range n.Pointers {
		if p.isShard() {
			chnd, err := p.loadChild(ctx, n.store, n.bitWidth)
			if err != nil {
				return err
			}

			if err := chnd.ForEach(ctx, f); err != nil {
				return err
			}
		} else {
			for _, kv := range p.KVs {
				if err := f(kv.Key, kv.Value); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
