package config

import "github.com/lechandonga/resilient-rpc-gateway-go/pkg/model"

// ErrNilConfig 包装统一的配置拒绝错误。
func ErrNilConfig() error { return model.ErrConfigRejected }
