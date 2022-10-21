package generics

import "context"

// MaxConcurrent limits the maximum number of executions in flight for a function by using a pool of tokens.
// Each time a request starts executing, a token is obtained - then subsequently released. The function is panic
// safe, and will release tokens cleanly through the use of a defer.
func MaxConcurrent[I any, O any](n int, execute func(ctx context.Context, in I) (O, error)) func(context.Context, I) (O, error) {
	tokens := make(chan bool, n)
	for i := 0; i < n; i++ {
		tokens <- true
	}

	return func(ctx context.Context, i I) (O, error) {
		<-tokens
		defer func() {
			tokens <- true
		}()

		return execute(ctx, i)
	}
}
