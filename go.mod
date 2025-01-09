module github.com/JCoupalK/go-pgdump

go 1.23.0

require (
	github.com/lib/pq v1.10.9
	golang.org/x/sync v0.8.0
)

// Retract old unstable versions
retract (

	// v0.2.0 contains breaking changes
	v0.2.0
	// v0.1.0-v0.1.9 contains critical bugs and should not be used
	[v0.1.0, v0.1.9]
)
