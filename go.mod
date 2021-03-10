module github.com/dsu-igeek/astrolabe-kopia

go 1.13

require (
	github.com/google/uuid v1.1.2
	github.com/kopia/kopia v0.7.3
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.7.0
	github.com/vmware-tanzu/astrolabe v0.8.1
)

replace github.com/vmware-tanzu/astrolabe => ../../vmware-tanzu/astrolabe

replace github.com/vmware-tanzu/velero => ../../vmware-tanzu/velero

replace github.com/kopia/kopia => ../../kopia/kopia
