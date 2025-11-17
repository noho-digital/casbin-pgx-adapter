package pgxadapter

var (
	insertColumns = []string{"ptype", "v0", "v1", "v2", "v3", "v4", "v5"}
	selectColumns = []string{"ptype", "v0", "v1", "v2", "v3", "v4", "v5"}

	colParams = map[int]string{
		0: "v0",
		1: "v1",
		2: "v2",
		3: "v3",
		4: "v4",
		5: "v5",
	}
)
