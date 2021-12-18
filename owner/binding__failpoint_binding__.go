
package owner

import "reflect"

type __failpointBindingType struct {pkgpath string}
var __failpointBindingCache = &__failpointBindingType{}

func init() {
	__failpointBindingCache.pkgpath = reflect.TypeOf(__failpointBindingType{}).PkgPath()
}
func _curpkg_(name string) string {
	return  __failpointBindingCache.pkgpath + "/" + name
}
