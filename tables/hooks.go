package tables

import (
	"context"
	"fmt"
)

// ChangeHook is a function that recieves an object in response to a pre or post change event.
type ChangeHook[T any] func(ctx context.Context, updatedRecord *T) error

// AddPreChangeHook adds a pre-change hook. These hooks do not fire for deletes.
func (t *tableManagerImpl[T]) AddPreChangeHook(hook ChangeHook[T]) {
	t.preHooks = append(t.preHooks, hook)
}

// AddPreDeleteHook adds a pre-delete hook. This will force an additional cost, in
// that we must retrieve the full record first before.
func (t *tableManagerImpl[T]) AddPreDeleteHook(hook ChangeHook[T]) {
	t.preDeleteHooks = append(t.preDeleteHooks, hook)
}

// AddPostChangeHook adds a post-change hook. Note that post-change hooks that fail
// will leave the base tables updated. These hooks do not fire for deletes.
func (t *tableManagerImpl[T]) AddPostChangeHook(hook ChangeHook[T]) {
	t.postHooks = append(t.postHooks, hook)
}

// runPreHooks runs pre-change hooks
func (t *tableManagerImpl[T]) runPreHooks(ctx context.Context, instance *T) error {
	for i, hook := range t.preHooks {
		err := hook(ctx, instance)
		if err != nil {
			return fmt.Errorf("error executing pre-hook at index %d: %w", i, err)
		}
	}

	return nil
}

// runPostHooks runs pre-change hooks
func (t *tableManagerImpl[T]) runPostHooks(ctx context.Context, instance *T) error {
	for i, hook := range t.postHooks {
		err := hook(ctx, instance)
		if err != nil {
			return fmt.Errorf("error executing post-hook at index %d: %w", i, err)
		}
	}

	return nil
}

// runPreDeleteHooks runs pre-delete hooks
func (t *tableManagerImpl[T]) runPreDeleteHooks(ctx context.Context, instance *T) error {
	if instance == nil {
		return nil
	}

	for i, hook := range t.preHooks {
		err := hook(ctx, instance)
		if err != nil {
			return fmt.Errorf("error executing pre-hook at index %d: %w", i, err)
		}
	}

	return nil
}
