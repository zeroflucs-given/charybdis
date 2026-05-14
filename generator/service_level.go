package generator

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/zeroflucs-given/charybdis/metadata"
)

func (g *DefinitionGenerator) CreateServiceLevel(ctx context.Context, name string, opts ...ServiceLevelOption) error {
	var ddl []metadata.DDLOperation
	d, err := SetServiceLevelExistenceDDL(name, "CREATE")
	if err != nil {
		return err
	}
	ddl = append(ddl, d...)

	d, err = UpdateServiceLevelDDL(name, opts...)
	if err != nil {
		return err
	}
	ddl = append(ddl, d...)

	return installDLL(ctx, g.logger, g.session, ddl)
}

func (g *DefinitionGenerator) UpdateServiceLevel(ctx context.Context, name string, opts ...ServiceLevelOption) error {
	var ddl []metadata.DDLOperation
	d, err := UpdateServiceLevelDDL(name, opts...)
	if err != nil {
		return err
	}
	ddl = append(ddl, d...)

	return installDLL(ctx, g.logger, g.session, ddl)
}

func (g *DefinitionGenerator) DeleteServiceLevel(ctx context.Context, name string) error {
	var ddl []metadata.DDLOperation
	d, err := SetServiceLevelExistenceDDL(name, "DROP")
	if err != nil {
		return err
	}
	ddl = append(ddl, d...)

	return installDLL(ctx, g.logger, g.session, ddl)
}

func (g *DefinitionGenerator) AttachServiceLevel(ctx context.Context, level string, role string) error {
	ddl, errDDL := ServiceLevelAttachmentDDL(level, role, "ATTACH")
	if errDDL != nil {
		return errDDL
	}
	return installDLL(ctx, g.logger, g.session, ddl)
}

func (g *DefinitionGenerator) DetachServiceLevel(ctx context.Context, level string, role string) error {
	ddl, errDDL := ServiceLevelAttachmentDDL(level, role, "DETACH")
	if errDDL != nil {
		return errDDL
	}
	return installDLL(ctx, g.logger, g.session, ddl)
}

type serviceLevelOpt struct {
	shares       *int32
	workloadType *WorkloadType
	timeout      *time.Duration
}

type ServiceLevelOption func(opt *serviceLevelOpt)

func collectServiceLevelOptions(opts []ServiceLevelOption) *serviceLevelOpt {
	o := &serviceLevelOpt{}
	for _, opt := range opts {
		opt(o)
	}
	return o
}

func WithShares(shares int32) ServiceLevelOption {
	return func(opt *serviceLevelOpt) {
		opt.shares = new(int32)
		*opt.shares = shares
	}
}

func WithWorkloadType(workloadType string) ServiceLevelOption {
	return func(opt *serviceLevelOpt) {
		opt.workloadType = new(WorkloadType)
		*opt.workloadType = WorkloadType(workloadType)
	}
}

func WithTimeout(timeout time.Duration) ServiceLevelOption {
	return func(opt *serviceLevelOpt) {
		opt.timeout = new(time.Duration)
		*opt.timeout = timeout
	}
}

type WorkloadType string

const (
	WorkloadTypeUnspecified WorkloadType = "unspecified"
	WorkloadTypeInteractive WorkloadType = "interactive"
	WorkloadTypeBatch       WorkloadType = "batch"
)

var validWorkloadTypes Set[WorkloadType]

func init() {
	validWorkloadTypes = SetOf(WorkloadTypeUnspecified, WorkloadTypeInteractive, WorkloadTypeBatch)
}

func (w WorkloadType) String() string {
	return strings.ToLower(string(w))
}

func (w WorkloadType) Normalise() WorkloadType {
	return WorkloadType(strings.TrimSpace(strings.ToLower(string(w))))
}

func (w WorkloadType) Validate() error {
	if !validWorkloadTypes.Has(w.Normalise()) {
		return fmt.Errorf("unknown workload type '%s': %w", w, ErrOutOfRange)
	}
	return nil
}
