module github.com/JCoupalK/go-pgdump

go 1.23.0

require (
	github.com/lib/pq v1.10.9
	golang.org/x/sync v0.8.0
)

// Retract old unstable versions
retract (
	[v1.0.0, v1.0.9]
	[v0.2.0, v0.2.9]
	[v0.1.0, v0.1.9]
)
