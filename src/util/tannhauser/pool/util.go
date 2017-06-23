package pool;

/* index pool */
type IndexPool struct {
  size int;
  index chan int;
}

func (p *IndexPool) InitIndexPool(size int) {
  p.index = make(chan int, size);
  for i := 0; i < size; i++ {
    p.index <- i;
  }
  p.size = size;
  return;
}

func (p *IndexPool) GetIndex() (int) {
  return <- p.index;
}

func (p *IndexPool) FreeIndex(index int) {
  p.index <- index;
}

