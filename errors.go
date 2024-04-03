package inkdb

import "fmt"

var (
	ErrSplotchRangeExceeded = fmt.Errorf("outside splotch range")
	ErrSplotchFull          = fmt.Errorf("splotch full already")
)
