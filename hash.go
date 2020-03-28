package hamt

import (
	"fmt"

	"github.com/spaolacci/murmur3"
)

// hashBits is a helper that allows the reading of the 'next n bits' as an integer.
// hashBits用来辅助读取下n个bit，作为一个整数
type hashBits struct {
	b        []byte // hash字节数组
	consumed int    // 消耗的位数
}

// 制作n位掩码，类似000111
func mkmask(n int) byte {
	return (1 << uint(n)) - 1
}

// Next returns the next 'i' bits of the hashBits value as an integer, or an
// error if there aren't enough bits.
// 取下i位代表的整数
func (hb *hashBits) Next(i int) (int, error) {
	// 先判断长度是否超过
	if hb.consumed+i > len(hb.b)*8 {
		return 0, fmt.Errorf("sharded directory too deep")
	}
	return hb.next(i), nil
}

// 长度没超过的情况下，返回后i位代表的整数（这里除以8，即按字节取，bitmap也因此不能超过8位）
func (hb *hashBits) next(i int) int {
	curbi := hb.consumed / 8
	leftb := 8 - (hb.consumed % 8)

	curb := hb.b[curbi]
	if i == leftb { // 剩余的正好是i位，该字节与掩码运算后直接返回
		out := int(mkmask(i) & curb)
		hb.consumed += i
		return out
	} else if i < leftb { // 剩余的多于i位，去掉不想要的高和低位
		a := curb & mkmask(leftb) // mask out the high bits we don't want
		b := a & ^mkmask(leftb-i) // mask out the low bits we don't want
		c := b >> uint(leftb-i)   // shift whats left down
		hb.consumed += i
		return int(c)
	} else { // 剩余的少于i位，用剩余的位，加上拼接下一个字节
		out := int(mkmask(leftb) & curb)
		out <<= uint(i - leftb)
		hb.consumed += leftb
		out += hb.next(i - leftb)
		return out
	}
}

// 产生64位哈希
var hash = func(val string) []byte {
	h := murmur3.New64()
	h.Write([]byte(val))
	return h.Sum(nil)
}
