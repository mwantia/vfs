package cmd

import "fmt"

type NoArgsValidator struct{}

func (NoArgsValidator) Validate(args []string) error {
	if len(args) > 0 {
		return fmt.Errorf("this command takes no arguments")
	}

	return nil
}

type ExactArgsValidator int

func (v ExactArgsValidator) Validate(args []string) error {
	if len(args) != int(v) {
		return fmt.Errorf("this command requires exactly %d argument(s), got %d", int(v), len(args))
	}

	return nil
}

type MinimumArgsValidator int

func (v MinimumArgsValidator) Validate(args []string) error {
	if len(args) < int(v) {
		return fmt.Errorf("this command requires at least %d argument(s), got %d", int(v), len(args))
	}

	return nil
}

type MaximumArgsValidator int

func (v MaximumArgsValidator) Validate(args []string) error {
	if len(args) > int(v) {
		return fmt.Errorf("this command accepts at most %d argument(s), got %d", int(v), len(args))
	}

	return nil
}

type RangeArgsValidator struct {
	Min int
	Max int
}

func (v RangeArgsValidator) Validate(args []string) error {
	if len(args) < v.Min || len(args) > v.Max {
		return fmt.Errorf("this command requires between %d and %d argument(s), got %d", v.Min, v.Max, len(args))
	}

	return nil
}

// ArbitraryArgs validates any number of arguments (always passes).
type ArbitraryArgsValidator struct{}

func (ArbitraryArgsValidator) Validate(args []string) error {
	return nil
}
