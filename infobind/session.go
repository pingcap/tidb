package infobind

type SessionBind struct {
	GlobalBindAccessor GlobalBindAccessor
	handle				Handle
}

func (s *SessionBind) SetBind(originSql string , infoBind *InfoBind){
	s.handle.Put(originSql , infoBind)
}

func (s *SessionBind) RemoveBind(originSql string) {
	s.handle.Delete(originSql)	//todo 这个地方是不是有问题，是否应该加上db，是否需要提供一个原子性的删除，加个锁啥的
}

func (s *SessionBind) GetBind(originSql string, defaultDb string) *InfoBind {
	return s.handle.Get(originSql)	//todo 这个地方是不是有问题，是否应该加上db
}

type GlobalBindAccessor interface {
	GetAllBindAccessor() (map[string]string, error)
	DropGlobalBind(name string, defaultDb string) error
	AddGlobalBind(originSql string, bindSql string, defaultDb string) error
}
