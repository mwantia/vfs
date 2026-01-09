package cmd

// CommandChain represents a sequence of pipelines connected by chaining operators
type CommandChain struct {
	Segments []*ChainSegment
}

// ChainSegment is a pipeline with an operator to the next segment
type ChainSegment struct {
	Pipeline *Pipeline
	Operator ChainOperator // How to connect to next segment
}

// ChainOperator defines how chains are connected
type ChainOperator int

const (
	ChainOpNone     ChainOperator = iota // Last segment
	ChainOpSequence                      // ; (always run next)
	ChainOpAnd                           // && (run next if success)
	ChainOpOr                            // || (run next if failure)
)

// String returns a string representation of the chain operator
func (op ChainOperator) String() string {
	switch op {
	case ChainOpNone:
		return "None"
	case ChainOpSequence:
		return "Sequence"
	case ChainOpAnd:
		return "And"
	case ChainOpOr:
		return "Or"
	default:
		return "Unknown"
	}
}
