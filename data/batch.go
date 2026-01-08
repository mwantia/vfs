package data

func BatchMetadata(items []Metadata, batch func(Metadata, int) Metadata) []Metadata {
	out := make([]Metadata, len(items))
	for i, item := range items {
		clone := item
		out[i] = batch(clone, i)
	}
	return out
}
