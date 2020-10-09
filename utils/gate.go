package utils

type Gate chan struct{}

func NewGate(max int) Gate {
	return make(Gate, max)
}

func (g Gate) Start() Gate {
	g <- struct{}{}
	return g
}

func (g Gate) Done() {
	<-g
}

func (g Gate) Do(f func()) {
	defer g.Start().Done()
	f()
}
