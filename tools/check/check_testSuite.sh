#!/bin/bash

exitCode=0
for testSuite in `find -name "*_test.go" | xargs grep -P "type test(.*)Suite" | awk '{print $2}'`; do
	# TODO: ugly regex
	# TODO: check code comment
	find -name "*_test.go" | xargs grep -P "_ = (check\.)?(Suite|SerialSuites)\((&?${testSuite}{|new\(${testSuite}\))" > /dev/null
	[[ $? != 0 ]] && find -name "*_test.go" | xargs grep "func (s \*${testSuite}) Test" > /dev/null
	[[ $? == 0 ]] && echo "${testSuite} is not enabled" && exitCode=1
done
exit ${exitCode}
