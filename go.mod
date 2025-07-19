module github.com/robinbryce/go-merklelog-provider-testing

go 1.24

replace (
	github.com/datatrails/go-datatrails-common => ../go-datatrails-common
	github.com/datatrails/go-datatrails-merklelog/massifs => ../go-datatrails-merklelog/massifs
	github.com/datatrails/go-datatrails-merklelog/massifs/storageschema => ../go-datatrails-merklelog/massifs/storageschema
	github.com/datatrails/go-datatrails-merklelog/mmr => ../go-datatrails-merklelog/mmr
)

require (
	github.com/datatrails/go-datatrails-common v0.30.0
	github.com/datatrails/go-datatrails-merklelog/massifs v0.0.0-00010101000000-000000000000
	github.com/datatrails/go-datatrails-merklelog/mmr v0.4.0
	github.com/google/uuid v1.6.0
	github.com/stretchr/testify v1.10.0
)

require (
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/fxamacker/cbor/v2 v2.7.0 // indirect
	github.com/ldclabs/cose/go v0.0.0-20221214142927-d22c1cfc2154 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/veraison/go-cose v1.1.0 // indirect
	github.com/x448/float16 v0.8.4 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	go.uber.org/zap v1.27.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)
