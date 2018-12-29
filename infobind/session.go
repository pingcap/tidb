package infobind

import "github.com/zhaoxiaojie0415/parser"

type SessionBind struct {
	GlobalBindAccessor GlobalBindAccessor
	bindCache *BindCache
}

func NewSessionBind() *SessionBind {
	return &SessionBind{}
}

func (s *SessionBind) SetBind(originSql string , newBindData *BindData){
	hash := parser.Digest(originSql)

	oldBindDataArr,ok := s.bindCache.Cache[hash]
	var newBindDataArr = make([] *BindData , 0)
	if ok {
		for pos, oldBindData := range oldBindDataArr {
			if oldBindData.BindRecord.OriginalSql == newBindData.BindRecord.OriginalSql && oldBindData.BindRecord.Db == newBindData.BindRecord.Db {
				newBindDataArr = append(newBindDataArr , oldBindDataArr[:pos]...)
				newBindDataArr = append(newBindDataArr , oldBindDataArr[pos+1:]...)
			}
		}
		newBindDataArr = append(newBindDataArr, newBindData)
	}

	newBindDataArr = append(newBindDataArr, newBindData)
	s.bindCache.Cache[hash] = newBindDataArr
}

func (s *SessionBind) RemoveBind(originSql string, defaultDb string) bool {
	hash := parser.Digest(originSql)

	oldBindDataArr,ok := s.bindCache.Cache[hash]
	var newBindDataArr = make([] *BindData , 0)
	if ok {
		for pos, oldBindData := range oldBindDataArr {
			if oldBindData.BindRecord.OriginalSql == originSql && oldBindData.BindRecord.Db == defaultDb {
				newBindDataArr = append(newBindDataArr , oldBindDataArr[:pos]...)
				newBindDataArr = append(newBindDataArr , oldBindDataArr[pos+1:]...)
			}
		}

		if len(newBindDataArr) != 0 {
			s.bindCache.Cache[hash] = newBindDataArr
		} else {
			delete(s.bindCache.Cache, hash)
		}

		return true
	}

	return false
}

func (s *SessionBind) GetBind(originSql string, defaultDb string) *BindData {
	hash := parser.Digest(originSql)

	oldBindDataArr,ok := s.bindCache.Cache[hash]
	if ok {
		for _, oldBindData := range oldBindDataArr {
			if oldBindData.BindRecord.OriginalSql == originSql && oldBindData.BindRecord.Db == defaultDb {
				return oldBindData
			}
		}
	}

	return nil
}

func (s *SessionBind) GetAllBind() map[string][]*BindData {
	return s.bindCache.Cache
}

type GlobalBindAccessor interface {
	GetAllBindAccessor() []*BindData
	DropGlobalBind(name string, defaultDb string) error
	AddGlobalBind(originSql string, bindSql string, defaultDb string) error
}
