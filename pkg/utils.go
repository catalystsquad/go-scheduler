package pkg

import "time"

func TimeToString(theTime time.Time) string {
	return theTime.Format(time.RFC3339Nano)
}
