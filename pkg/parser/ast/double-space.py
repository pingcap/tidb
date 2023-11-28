f = open('ddl.go')
lines = f.readlines()
for i in range(len(lines) - 1):
	if '"= "' in lines[i]:
		tokens = lines[i-1].split('"')
		if len(tokens) == 3:
			print('"'+tokens[1]+'": true,')
