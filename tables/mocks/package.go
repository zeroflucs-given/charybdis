package mocks

//go:generate go tool mockgen -source ../interface.go -destination ./mockgen.go -package mocks -exclude_interfaces ManagerOption
