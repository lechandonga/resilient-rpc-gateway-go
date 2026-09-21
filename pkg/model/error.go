package model

// GovError 是带稳定原因码的治理层错误，便于 errors.Is 与日志解释。
type GovError struct {
	Code    string
	Message string
}

func newGovError(code, msg string) *GovError {
	return &GovError{Code: code, Message: msg}
}

func (e *GovError) Error() string { return e.Code + ": " + e.Message }

// Is 保证被包装后仍可被 errors.Is 命中。
func (e *GovError) Is(target error) bool {
	t, ok := target.(*GovError)
	if !ok {
		return false
	}
	return e.Code == t.Code
}
