package schema

type Scope struct {
	Database        string
	RetentionPolicy string
	Measurement     string
}

type Snapshot struct {
	Database        string
	RetentionPolicy string
	Measurements    []Measurement
}

type Measurement struct {
	Name   string
	Fields []Field
	Tags   []Tag
}

type Field struct {
	Name string
	Type string
}

type Tag struct {
	Name string
}
