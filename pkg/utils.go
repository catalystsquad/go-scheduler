package pkg

import "time"

func TimeToString(theTime time.Time) string {
	return theTime.Format(time.RFC3339Nano)
}

func StringPointer(theString string) *string {
	if theString == "" {
		return nil
	}
	return &theString
}

func ISONanoString(theTime time.Time) string {
	return theTime.Format(time.RFC3339Nano)
}
