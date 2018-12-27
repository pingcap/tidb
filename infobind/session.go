package infobind

import "github.com/pingcap/tidb/util/kvcache"

type SessionBind struct {
	GlobalBindAccessor GlobalBindAccessor
	LocalBindCache *kvcache.SimpleMap
}

func NewSessionBind() *SessionBind {
	return &SessionBind{}
}

func (s *SessionBind) SetBind(originSql string , bindData *BindData){
	bindDataArrInterface,ok := s.LocalBindCache.Get(originSql)
	var newBindDataArr = make([] *BindData , 0)
	if ok {
		oldBindDataArr := bindDataArrInterface.([] *BindData)

		for pos,bindDataInArr := range oldBindDataArr {
			if bindDataInArr.DefaultDB == bindData.DefaultDB {
				newBindDataArr = append(newBindDataArr , oldBindDataArr[:pos]...)
				newBindDataArr = append(newBindDataArr , oldBindDataArr[pos+1:]...)
			}
		}
		oldBindDataArr = append(oldBindDataArr, bindData)
	}

	newBindDataArr = append(newBindDataArr, bindData)
	s.LocalBindCache.Put(originSql , newBindDataArr)
}

func (s *SessionBind) RemoveBind(originSql string, defaultDb string) {
	bindDataArrInterface,ok := s.LocalBindCache.Get(originSql)
	var newBindDataArr = make([] *BindData , 0)
	if ok {
		oldBindDataArr := bindDataArrInterface.([] *BindData)

		for pos,bindDataInArr := range oldBindDataArr {
			if bindDataInArr.DefaultDB == defaultDb {
				newBindDataArr = append(newBindDataArr , oldBindDataArr[:pos]...)
				newBindDataArr = append(newBindDataArr , oldBindDataArr[pos+1:]...)
			}
		}
	}

	s.LocalBindCache.Put(originSql , newBindDataArr)
}

func (s *SessionBind) GetBind(originSql string, defaultDb string) *BindData {
	bindDataArrInterface,ok := s.LocalBindCache.Get(originSql)
	if ok {
		oldBindDataArr := bindDataArrInterface.([] *BindData)

		for _,bindDataInArr := range oldBindDataArr {
			if bindDataInArr.DefaultDB == defaultDb {
				return bindDataInArr
			}
		}
	}

	return nil
}

func (s *SessionBind) GetAllBind() []*BindData {
	bindDataArr := make([] *BindData,0)

	for _,bindData := range s.LocalBindCache {

	}
}

type GlobalBindAccessor interface {
	GetAllBindAccessor() (map[string]string, error)
	DropGlobalBind(name string, defaultDb string) error
	AddGlobalBind(originSql string, bindSql string, defaultDb string) error
}
