package gcp

import "github.com/dogmatiq/ferrite"

var ProjectID = ferrite.String("GCP_PROJECT_ID", "gcp project id").Required()
var CredentialFileName = ferrite.String("GCP_CREDENTIAL_FILENAME", "gcp credential filename").Required()
