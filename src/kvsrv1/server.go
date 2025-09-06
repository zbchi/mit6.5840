package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ValueVersion struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu      sync.Mutex
	dataMap map[string]ValueVersion
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	kv.dataMap = make(map[string]ValueVersion)
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	valueVersion, ok := kv.dataMap[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
	} else {
		reply.Value = valueVersion.Value
		reply.Version = valueVersion.Version
		reply.Err = rpc.OK
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.Version > 0 {
		vv, ok := kv.dataMap[args.Key]
		if !ok { //not find
			reply.Err = rpc.ErrNoKey
		} else if args.Version != vv.Version { //not match
			reply.Err = rpc.ErrVersion
		} else { //match,write in
			vv.Version++
			kv.dataMap[args.Key] = ValueVersion{
				Value:   args.Value,
				Version: vv.Version,
			}
			reply.Err = rpc.OK
		}
	} else if args.Version == 0 {
		_, ok := kv.dataMap[args.Key]
		if ok {
			reply.Err = rpc.ErrVersion
		} else {
			kv.dataMap[args.Key] = ValueVersion{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
		}
	}

}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
