// pkg/version/version.go

package version

import "fmt"

var (
    version      = "0.1-dev"
    revision     = "$Format:%h$"
    revisionDate = "$Format:%as$"
)

// Version returns the version in format - `VERSION (REVISIONDATE REVISION)`
// value is assigned in Makefile
func Version() string {
    return fmt.Sprintf("%v (%v %v)", version, revisionDate, revision)
}
