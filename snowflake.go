package main

import (
	"errors"
	"sync"
	"time"
)

// 雪花算法, 往往生成 64bit 整数返回
// 首位一般不使用，也可以作为符号位
// 第 2 bit 到 42 bit, 41 位作为毫秒级时间戳
// 第 43 bit 到 52 bit, 10 位作为机器号，或拆分成两个 5 bit 分别作为 IDC 号和 IDC 下的机器号
// 第 53 bit 到 64 bit, 最后 12 bit 作为每毫秒产生的序列号(每毫秒内递增)
// 整个逻辑非常简单，初始化生成器，确定生成器的 IDC 号和机器号
// 生成时，同一毫秒则增长序列，新毫秒则重置序列，序列超了则等待下一毫秒并重置序列
const (
	sequenceIDBits = 12                             // 序列号，占用的 bit 位
	machineIDBits  = 5                              // 机器号占用的 bit 位
	idcIDBits      = 5                              // IDC 号占用的 bit 位
	machineIDShift = sequenceIDBits                 // 机器号的偏移量
	idcIDShift     = machineIDBits + machineIDShift // IDC 号的偏移量
	unixMilliShift = idcIDBits + idcIDShift         // 时间戳的偏移量
	maxSequenceID  = ^(-1 << sequenceIDBits)        // 序列号的最大值 可以获得 sequenceIDBits 下的最数值，比如 bit=5 时，最大为 31
	maxMachineID   = ^(-1 << machineIDBits)         // 机器号的最大值
	maxIDCID       = ^(-1 << idcIDBits)             // IDC 号的最大值
	epoch          = 1669046400000                  // 2022-11-22 00:00:00 的毫秒时间戳，开始使用时间
)

var (
	ErrInvaildIDCID     = errors.New("IDGenerator: input invaild IDC ID")
	ErrInvaildMachineID = errors.New("IDGenerator: input invaild machine ID")
	ErrClockBack        = errors.New("IDGenerator: clock turn back, stop generating to avoid generating repeated ID")
)

// IDGenerator 雪花算法 ID 生成器
type IDGenerator struct {
	lastMilli  int64      // 上一次生成 ID 的毫秒时间
	sequenceID int64      // 本毫秒内的序列号
	machineID  int64      // 本 IDGenerator 所属机器号
	IDCID      int64      // 本 IDGenerator 所属 IDC 号
	mutex      sync.Mutex // 锁，用于并发生成 ID 时不会冲突
}

// NewIDGenerator 生成一个基于标准雪花算法的 ID 生成器
func NewIDGenerator(idcID, machineID int64) (*IDGenerator, error) {
	if idcID > maxIDCID || idcID < 0 {
		return nil, ErrInvaildIDCID
	}
	if machineID > maxMachineID || machineID < 0 {
		return nil, ErrInvaildMachineID
	}
	return &IDGenerator{
		lastMilli:  -1,
		sequenceID: 0,
		machineID:  machineID,
		IDCID:      idcID,
	}, nil
}

// Generate 生成一个 ID
func (g *IDGenerator) Generate() (int64, error) {
	g.mutex.Lock()
	defer g.mutex.Unlock()
	now := g.now()
	// 机器时钟回拨才会导致 now 当前毫秒时间戳小于上一次生成 ID 的毫秒时间戳
	if now < g.lastMilli {
		return -1, ErrClockBack
	}
	// 当毫秒时间相等时，改变序列号即可，顺便考虑下序列号超了的情况
	// 当 now 当前毫秒时间戳已经超过上一次生成 ID 当毫秒时间戳，重置 seqID
	if now == g.lastMilli {
		g.sequenceID++
		if g.sequenceID > maxSequenceID {
			// 若同一毫秒内序列号已经超了，则等待到下一毫秒并且重置 seqID
			now = g.tilNextMilli(now)
			g.sequenceID = 0
		}
	} else if now > g.lastMilli {
		g.sequenceID = 0
	}
	g.lastMilli = now
	return (now-epoch)<<unixMilliShift | g.IDCID<<idcIDShift | g.machineID<<machineIDShift | g.sequenceID, nil
}

// 获取当前的毫秒时间戳
func (g *IDGenerator) now() int64 {
	return time.Now().UnixMilli()
}

// 等待到下一毫秒
func (g *IDGenerator) tilNextMilli(now int64) int64 {
	for now <= g.lastMilli {
		now = g.now()
	}
	return now
}
