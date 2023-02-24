package distribute_framework

type GlobalTaskMeta interface {
	Serialize() []byte
	GetType() string
}

type globalBasicTaskMeta struct {
}

func (g *globalBasicTaskMeta) Serialize() []byte {
	return []byte{}
}

func (g *globalBasicTaskMeta) GetType() string {
	return "basic"
}
