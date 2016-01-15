package plan

type AccessMethodType int

const (
	AcccessTableScan AccessMethodType = iota + 1
	AccessIndexScan
	AccessIndexOnly
)

type Scan struct {
	AccessMethod AccessMethodType
	UseIndex
}