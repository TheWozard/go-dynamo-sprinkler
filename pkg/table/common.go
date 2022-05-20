package table

var (
	PrimaryDestination = Destination{
		StatusAttribute: "primary-status",
		StatusIndex:     "primary-status-index",
	}
	SecondaryDestination = Destination{
		StatusAttribute: "secondary-status",
		StatusIndex:     "secondary-status-index",
	}

	DefaultConfig = Config{
		TableName: "go-sprinkler",
		Attributes: Attributes{
			ID:         "id",
			Timestamp:  "timestamp",
			Sum:        "sum",
			Providence: "providence",
		},
		Destinations: []*Destination{
			&PrimaryDestination,
			&SecondaryDestination,
		},
	}
)

// Destination defines a destination for records out of the sprinkler
type Destination struct {
	StatusAttribute string
	StatusIndex     string
}

type Attributes struct {
	ID         string
	Timestamp  string
	Sum        string
	Providence string
}

type Config struct {
	TableName    string
	Attributes   Attributes
	Destinations []*Destination
}
