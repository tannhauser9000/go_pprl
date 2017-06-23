package lock;

import pool "util/tannhauser/pool";

type Lock struct {
  lock *pool.IndexPool;
}

type RWLock struct {
  rlock *Lock;
  wlock *Lock;
  reader int;
}

func GetLock() (*Lock, error) {
  l := &Lock{};
  (*l).lock = &pool.IndexPool{};
  (*l).lock.InitIndexPool(1);
  return l, nil;
}

func (l *Lock) Lock() {
  (*l).lock.GetIndex();
}

func (l *Lock) Unlock() {
  (*l).lock.FreeIndex(0);
}

func GetRWLock() (*RWLock, error) {
  rwlock := &RWLock{};
  var err error;
  (*rwlock).rlock, err = GetLock();
  if err != nil {
    return nil, err;
  }
  (*rwlock).wlock, err = GetLock();
  if err != nil {
    return nil, err;
  }
  (*rwlock).reader = 0;
  return rwlock, nil;
}

func (rwlock *RWLock) RLock() {
  (*rwlock).rlock.Lock();
  (*rwlock).reader++;
  if (*rwlock).reader == 1 {
    (*rwlock).wlock.Lock();
  }
  (*rwlock).rlock.Unlock();
}

func (rwlock *RWLock) RUnlock() {
  (*rwlock).rlock.Lock();
  (*rwlock).reader--;
  if (*rwlock).reader == 0 {
    (*rwlock).wlock.Unlock();
  }
  (*rwlock).rlock.Unlock();
}

func (rwlock *RWLock) Lock() {
  (*rwlock).wlock.Lock();
}

func (rwlock *RWLock) Unlock() {
  (*rwlock).wlock.Unlock();
}

