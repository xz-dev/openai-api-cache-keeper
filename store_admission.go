package main

import "context"

func (s *store) acquireMutation(ctx context.Context) error {
	s.mu.once.Do(func() { s.mu.token = make(chan struct{}, 1) })
	select {
	case s.mu.token <- struct{}{}:
		select {
		case <-s.closing:
			s.mu.Unlock()
			return errStoreClosed
		default:
		}
		if err := ctx.Err(); err != nil {
			s.mu.Unlock()
			return err
		}
		return nil
	case <-s.closing:
		return errStoreClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}
