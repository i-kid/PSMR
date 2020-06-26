// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cliproxy

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"time"

	"go.etcd.io/etcd/etcdserver/api/rafthttp"
	"go.etcd.io/etcd/etcdserver/api/snap"
	stats "go.etcd.io/etcd/etcdserver/api/v2stats"
	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// A key-value stream backed by raft
type raftNode struct {
	index       int                      //the index of multicast
	proposeC    <-chan string            // proposed messages (k,v)
	confChangeC <-chan raftpb.ConfChange // proposed cluster config changes
	commitC     chan<- *string           // entries committed to log (k,v)
	errorC      chan<- error             // errors from raft session

	id          int      // client ID for raft session
	peers       []string // raft peer URLs
	join        bool     // node is joining an existing cluster
	waldir      string   // path to WAL directory
	snapdir     string   // path to snapshot directory
	GetSnapshot func() ([]byte, error)
	lastIndex   uint64 // index of log at start

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *rafthttp.Transport
	stopc     chan struct{} // signals proposal channel closed
	httpstopc chan struct{} // signals http server to shutdown
	httpdonec chan struct{} // signals http server shutdown complete
}

var defaultSnapshotCount uint64 = 10000

// NewRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewRaftNode(i int, id int, peers []string, join bool, GetSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {
	commitC := make(chan *string, 100000)
	errorC := make(chan error)
	rc := &raftNode{
		index:            i,
		proposeC:         proposeC,
		confChangeC:      confChangeC,
		commitC:          commitC,
		errorC:           errorC,
		id:               id,
		peers:            peers,
		join:             join,
		waldir:           fmt.Sprintf("cliproxy-%d-%d", i, id),
		snapdir:          fmt.Sprintf("cliproxy-%d-%d-snap", i, id),
		GetSnapshot:      GetSnapshot,
		snapCount:        defaultSnapshotCount,
		stopc:            make(chan struct{}),
		httpstopc:        make(chan struct{}),
		httpdonec:        make(chan struct{}),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	// must save the snapshot index to the WAL before saving the
	// snapshot to maintain the invariant that we only Open the
	// wal at previously-saved snapshot indexes.
	// 根据快照的元数据，创建 walpb.Snapshot 实例
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	// WAL 会将上述快照的元数据信息封装成一条日志记录下来， WAL 的实现在后面的章节中详细介绍
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	//  将新快照数据写入快照文件中
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	// 根据快照的元数据信息，释放一些无用的 WAL 日志文件的句柄， WAL 的实现在后面的章节中详细介绍
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	// 长度检测
	if len(ents) == 0 {
		return ents
	}
	firstIdx := ents[0].Index
	// 检测 firstIndex 是否合法
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d]+1", firstIdx, rc.appliedIndex)
	}
	// 过滤掉已经被应用过的 Entry 记录
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return nents
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			// 如果 Entry 记录的 Data 为空 ， 则直接忽略该条 Entry 记录
			if len(ents[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(ents[i].Data)
			select { // 将数据写入 commitC 通道， kvstore 会读取从其中读取并记录相应的 KV 值
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}

		case raftpb.EntryConfChange:
			// 将 EntryConfChange 类型的记录封装成 ConfChange
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			// 将 ConfChange 实例传入底层的 etcd-raft 组件，其中的处理过程在前面已经详细分析过了
			rc.confState = *rc.node.ApplyConfChange(cc)
			// 除了 etcd-raft 纽件中需要创建(或删除)对应的 Progress 实例 ，
			// 网络层也需要做出相应的调整，即添加(或删除)相应的 Peer 实例
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(types.ID(cc.NodeID), []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(types.ID(cc.NodeID))
			}
		}
		// 处理完成之后，更新 raftNode 记录的已应用位置，该值在过滤已应用记录的 entriesToApply()
		// 方法及后面即将介绍的 maybeTriggerSnapshot() 方法中都有使用
		// after commit, update appliedIndex
		rc.appliedIndex = ents[i].Index

		// 此次反用的是否为重放的 Entry 记录，如采是，且重放完成，
		// 则使用 cornrnitC 通道通知 kvstore
		// special nil commit to signal replay has finished
		if ents[i].Index == rc.lastIndex {
			select {
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("cliproxy: error loading snapshot (%v)", err)
	}
	return snapshot
}

// openWAL returns a WAL ready for reading.
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	// 检测 WAL 日志目录是否存在，如果不存在进行创建
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("cliproxy: cannot create dir for wal (%v)", err)
		}
		// 新建 WAL 实例，其中会创建相应目录和一个空的 WAL 日志文件
		w, err := wal.Create(zap.NewExample(), rc.waldir, nil)
		if err != nil {
			log.Fatalf("cliproxy: create wal error (%v)", err)
		}
		// 关闭 WAL, 其中包括各种关闭目录、文件和相关的 goroutine
		w.Close()
	}

	// 创建 walsnap.Snapshot 实例并初始化其 Index 字段和 Term 字段
	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(zap.NewExample(), rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("cliproxy: error loading wal (%v)", err)
	}

	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *raftNode) replayWAL() *wal.WAL {
	// 读取快照文件，该方法会调用 snapshotter.Load() 方法完成快照文件的读取
	log.Printf("replaying WAL of member %d", rc.id)
	snapshot := rc.loadSnapshot()
	// 根据读取到的 Snapshot 实例的元数据创建 WAL 实例
	w := rc.openWAL(snapshot)
	// 读取快照数据之后的全部 WAL 日志数据，并获取状态信息
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("cliproxy: failed to read WAL (%v)", err)
	}
	// 创建 MemoryStorage 实例
	rc.raftStorage = raft.NewMemoryStorage()
	// 将快照的数据加载到 MemoryStorage 中
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	// 将读取 WAL 日志之后得到的 HardState 加载到 MemoryStorage 中
	rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	// 将读取的 WAL 日志得到的 Entry 记录加载到 MemoryStorage 中
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	// 快照之后存在已经持久化的 Entry 记录，这些记录需要回放到上层应用的状态机中
	if len(ents) > 0 {
		// 更新 raftNode.lastIndex，记录回放结束的位置
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		// 快照之后不存在持久化的 Entry 记录，则向 commitC 中写入 nil
		// 当 WAL 日志全部回放完成，也会向 commitC 写入 nil 作为信号
		rc.commitC <- nil
	}
	return w
}

func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) startRaft() {
	// 检测到 snapdir 字段指定的目录是否存在，该目录用于存放定期生成的快照数据；
	// 若 snapdir 目录不存在，则进行创建；若创建失败，则输出异常日志并终止程序
	if !fileutil.Exist(rc.snapdir) {
		if err := os.Mkdir(rc.snapdir, 0750); err != nil {
			log.Fatalf("cliproxy: cannot create dir for snapshot (%v)", err)
		}
	}
	// 1. 创建 Snapshot 实例，该 Snapshot 实例会通过 snapshotterReady 通道返回给上层
	// 应用 Snapshotter 实例提供了读写快照文件的功能
	rc.snapshotter = snap.New(zap.NewExample(), rc.snapdir)
	rc.snapshotterReady <- rc.snapshotter

	//2. 创建 WAL 实例，然后加载快照并回放 WAL 日志
	oldwal := wal.Exist(rc.waldir) // 检测 waldir 目录下是否存在旧的 WAL 日志文件
	rc.wal = rc.replayWAL()        // 在 replyWAL() 方法中会先加载快照数据，然后重访 WAL 日志文件

	// 3. 创建 raft.Config 实例
	rpeers := make([]raft.Peer, len(rc.peers))
	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
	}
	c := &raft.Config{
		ID:                        uint64(rc.id),
		ElectionTick:              10,             // 选举超时
		HeartbeatTick:             1,              // 心跳超时
		Storage:                   rc.raftStorage, // 持久化存储。与 etcd-raft 模块中的 raftLog.storage 共享同一个 MemoryStorage 实例
		MaxSizePerMsg:             1024 * 1024,    //每条消息的最大长度
		MaxInflightMsgs:           256,            // 已发送但是未收到响应的消息上限个数
		MaxUncommittedEntriesSize: 1 << 30,
	}
	// 4. 初始化底层的 etcd-raft 模块，这里会根据 WAL 日志的回放情况，
	// 判断当前节点是首次启动还是重新启动
	if oldwal {
		rc.node = raft.RestartNode(c)
	} else {
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}
	// 5. 创建 Transport 实例并启动，他负责 raft 节点之间的网络通信服务
	rc.transport = &rafthttp.Transport{
		Logger:      zap.NewExample(),
		ID:          types.ID(rc.id),
		ClusterID:   0x1000,
		Raft:        rc,
		ServerStats: stats.NewServerStats("", ""),
		LeaderStats: stats.NewLeaderStats(strconv.Itoa(rc.id)),
		ErrorC:      make(chan error),
	}

	// 启动网络服务相关组件
	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(types.ID(i+1), []string{rc.peers[i]})
		}
	}
	// 7. 启动一个goroutine，其中会监听当前节点与集群中其他节点之间的网络连接
	go rc.serveRaft()
	// 8. 启动后台 goroutine 处理上层应用与底层 etcd-raft 模块的交互
	go rc.serveChannels()
}

// stop closes http, closes all channels, and stops raft.
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	// 对快照数据进行一系列检测
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d]", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}

	// 使用 commitC 远远远知上层应用加载新 生成的快照数据
	rc.commitC <- nil // trigger Kvstore to load snapshot

	// 记录新快照的元数据
	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) maybeTriggerSnapshot() {
	// 检测处理的记录数是否足够，如果不足，则直接返回
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	// 获取快照数据，在 raftexample 示例中是获取 kvstore 中记录的全部键位对数据
	data, err := rc.GetSnapshot()
	if err != nil {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
		log.Panic(err)
	}
	// 创建 Snapshot 实例 同时也会将快照和元数据更新到 raftLog.MernoryStorage 中
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
		panic(err)
	}
	// 保存快照数据，raftNode.saveSnap() 方法在前面 已经介绍过了
	if err := rc.saveSnap(snap); err != nil {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
		panic(err)
	}

	compactIndex := uint64(1)
	// 计算压缩的位置， 压缩之后，该位置之前的全部记录都会被抛弃
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	// 压缩 raftLog 中保存的 Entry 记录， MemoryStorage.Compact()方法后续会讲到
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	// 前面介绍的 raftNode.replayWAL() 方法读取了快照数据、 WAL日志等信息，并记录到了
	// raftNode.raftStorage中 。这里是获取快照数据和快照的元数据
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		log.SetFlags(log.Lshortfile | log.LstdFlags)
		panic(err)
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

	defer rc.wal.Close()

	// 创建一个每隔 lOOms 触发一次的定时器，那么在逻辑上，lOOms 即是 etcd-raft 组件的最小时间单位 ，
	// 该定时器每触发一次，则逻辑时钟推进一次
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// send proposals over raft
	// 单独启 动一个 goroutine 负责将 proposeC、 confChangeC 远远上接收到
	// 的数据传递给 etcd-raft 组件进行处理
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					// blocks until accepted by raft state machine
					// 如采发生异常，则将 raftNode .proposeC 字段置空，当前循环及
					// 整个 goroutine都会结束，通过 node.Propose()方法，
					// 将数据传入底层 etcd-raft 纽件进行处理

					rc.node.Propose(context.TODO(), []byte(prop))
				}
				// 收到上层应用通过 confChangeC远远传递过来的数据
			case cc, ok := <-rc.confChangeC:
				if !ok {
					// 如果发生异常，则将 raftNode.confChangeC 字段置空
					rc.confChangeC = nil
				} else {
					// 统计集群变更请求的个数，并将其作为 ID
					confChangeCount++
					cc.ID = confChangeCount
					// 通过 node. ProposeConfChange() 方法，将数据传入反层 etcd-raft 纽件进行处理
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		//  关闭 stopc 通道，触发 rafeNode.stop() 方法的调用
		close(rc.stopc)
	}()

	// event loop on raft state machine updates
	// 该循环主要负责处理底层 etcd-raft 纽件返回的 Ready 数据
	for {
		select {
		// 上述 ticker 定时器触发一次，即会推进 etcd-raft 组件的逻辑时钟
		case <-ticker.C:
			rc.node.Tick()

		// store raft entries to wal, then publish over commit channel
		// 读取 node.readyc 通道，前面介绍 etcd-raft 组件时也提到 ，
		// 该通道是 etcd-raft 组件与上层应用交互的主要遥远之一，
		// 其中传递的 Ready 实例也封装了很多信息
		case rd := <-rc.node.Ready():
			// 将当前 etcd raft 组件的状态信息，以及待持久化的 Entry 记录先记录到 WAL 日志文件中，
			// 即使之后宕机，这些信息也可以在节点下次启动时，通过前面回放 WAL 日志的方式进行恢复
			// WAL 记录日志的具体实现， 在后面的章节中做详细介绍，这里不做过多描述
			rc.wal.Save(rd.HardState, rd.Entries)
			// 检测到 etcd-raft 组件生成了新的快照数据
			if !raft.IsEmptySnap(rd.Snapshot) {
				// 将新的快照数据写入快照文件中
				rc.saveSnap(rd.Snapshot)
				// 将新快照持久化到 raftStorage, MemoryStorage 的实现在后面的章节详细介绍
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				// 通知上层应用加载新快照
				rc.publishSnapshot(rd.Snapshot)
			}
			// 将待持久化的 Entry 记录追加到 raftStorage 中完成持久化
			rc.raftStorage.Append(rd.Entries)
			// 将待发送的消息发送到指定节点， Transport 的具体实现在后面的章节中做介绍，这里不做过多描述
			rc.transport.Send(rd.Messages)
			// 将已提交、待应用的 Entry 记录应用到上层应用的状态机中，异常处理(略)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			// 随着节点的运行， WAL 日志量和 raftLog.storage 中的 Entry 记录会不断增加 ，
			// 所以节点每处理 10000 条(默认值) Entry 记录，就会触发一次创建快照的过程，
			// 同时 WAL 会释放一些日志文件的句柄，raftLog.storage 也会压缩其保存的 Entry 记录
			rc.maybeTriggerSnapshot()
			// 上层应用处理完该 Ready 实例，通知 etcd-raft 纽件准备返回下一个 Ready 实例
			rc.node.Advance()

			// 处理网络异常
		case err := <-rc.transport.ErrorC:
			// 关闭与集群中其他节点的网络连接
			rc.writeError(err)
			return
			// 处理关闭命令
		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	// 获取当前节点的 URL 地址
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("cliproxy: Failed parsing URL (%v)", err)
	}
	// 创建 stoppableListener 实例，stoppableListener 继承了 net.TCPListener
	// 接口，它会与 http.Server 配合实现对当前节点的 URL 地址进行监听
	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("cliproxy: Failed to listen rafthttp (%v)", err)
	}
	// 创建 http.Server 实例，它会通过上面的 stoppableListener 实例监听当前的 URL 地址
	// stoppableListener.Accept() 方法监听到新的连接到来时，会创建对应的 net.Conn 实例，
	// http.Server 会为每个连接创建单独的 goroutine 处理，每个请求都会由 http.Server.Handler
	// 处理。这里的 Handler 是由 rafthttp.Transporter 创建的，后面详细介绍 rafthttp.Transporter
	// 的具体实现。另外需要读者了解的是 http.Server.Serve()方法会一直阻塞，直到 http.Server关闭

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("cliproxy: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool                           { return false }
func (rc *raftNode) ReportUnreachable(id uint64)                          {}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {}
