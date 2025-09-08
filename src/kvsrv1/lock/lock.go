package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck   kvtest.IKVClerk
	name string
	me   string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, name: l, me: kvtest.RandValue(8)}
	return lk
}

func (lk *Lock) Acquire() {
	i := 0
	for {
		value, version, err := lk.ck.Get(lk.name)
		fmt.Printf("[%s,%s] Acquire try %d, value: %s\n", lk.me, lk.name, i, value)
		if err == rpc.ErrNoKey { // free lock
			lk.ck.Put(lk.name, lk.me, 0)
			fmt.Printf("[%s,%s] Acquire success (no key)\n", lk.me, lk.name)
			break
		} else if value == "" { // free lock
			err := lk.ck.Put(lk.name, lk.me, version)
			if err != rpc.OK {
				fmt.Printf("[%s,%s] Acquire put failed: %v\n", lk.me, lk.name, err)
			} else {
				fmt.Printf("[%s,%s] Acquire success\n", lk.me, lk.name)
				break
			}
		} //retry
		time.Sleep(50 * time.Millisecond)

		i++
	}
}

func (lk *Lock) Release() {
	fmt.Printf("[%s,%s] Release start\n", lk.me, lk.name)
	value, version, err := lk.ck.Get(lk.name)
	if err == rpc.ErrNoKey {
		fmt.Printf("[%s,%s] Release failed: ErrNoKey\n", lk.me, lk.name)
		return
	} else if value != lk.me {
		fmt.Printf("[%s,%s] Release failed: not owner\n", lk.me, lk.name)
		return
	} else if value == lk.me {
		err := lk.ck.Put(lk.name, "", version)
		if err != rpc.OK {
			fmt.Printf("[%s,%s] Release put failed: %v\n", lk.me, lk.name, err)
		} else {
			fmt.Printf("[%s,%s] Release success\n", lk.me, lk.name)
		}
	}
}
