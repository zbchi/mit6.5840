package kvsrv

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		args := rpc.GetArgs{Key: key}
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			return reply.Value, reply.Version, reply.Err
		} else {
			fmt.Printf("RPC timeout ,retry\n")
			time.Sleep(100 * time.Millisecond)
		}

	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	is_retry := false
	for {
		args := rpc.PutArgs{
			Key:     key,
			Value:   value,
			Version: version,
		}
		reply := rpc.PutReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			if is_retry && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			return reply.Err
		} else {
			fmt.Printf("RPC timeout ,retry\n")
			time.Sleep(100 * time.Millisecond)
		}
		is_retry = true
	}
}

/*
关键点：

客户端 Clerk 必须不断重试 RPC，直到收到回复。
Call() 返回 true 表示收到了回复，false 表示超时（没收到）。

注意：
	请求丢失：重发即可，服务器会收到并执行。
	回复丢失：客户端会重发请求，服务器可能会执行两次。

Get：无副作用，安全。
Put：安全，因为有 version 检查，第二次会返回 ErrVersion。

特殊情况：
	如果 Clerk 收到的是 重试 Put 的 ErrVersion，它无法判断 Put 是否执行过：
	可能是第一次成功但回复丢了。
	也可能是并发被别人写入导致失败。
	这种情况 Clerk 必须返回 rpc.ErrMaybe 给应用（表示不确定是否执行过）。
*/
