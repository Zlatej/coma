module github.com/zlatej/coma

go 1.25

retract v0.1.0 // Wait can panic when called concurrently; superseded by the mutex/cond rewrite
