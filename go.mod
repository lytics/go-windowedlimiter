module github.com/vitaminmoo/go-slidingwindow

go 1.23.1

//replace github.com/fgrosse/zaptest => /home/vitaminmoo/repos/zaptest
replace github.com/fgrosse/zaptest => github.com/vitaminmoo/zaptest v1.2.2

require (
	github.com/fgrosse/zaptest v1.2.1
	github.com/stretchr/testify v1.9.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	github.com/go-redis/redismock/v9 v9.2.0 // indirect
	github.com/puzpuzpuz/xsync v1.5.2 // indirect
	github.com/yuseferi/zax/v2 v2.3.2 // indirect
	go.uber.org/multierr v1.11.0 // indirect
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/redis/go-redis/v9 v9.7.0
	go.uber.org/zap v1.27.0
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
