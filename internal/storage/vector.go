// internal/storage/vector.go
package storage

import (
	"fmt"
	"math"
)

// VectorDistance represents distance metrics for vectors
type VectorDistance string

const (
	DistanceCosine    VectorDistance = "COSINE"
	DistanceL2        VectorDistance = "L2"
	DistanceInnerProd VectorDistance = "INNER_PRODUCT"
)

// CosineSimilarity calculates cosine similarity between two vectors
// Returns a value between -1 and 1 (1 = identical, 0 = orthogonal, -1 = opposite)
func CosineSimilarity(a, b *Vector) (float64, error) {
	if a == nil || b == nil {
		return 0, fmt.Errorf("vectors cannot be nil")
	}

	if a.Dimensions != b.Dimensions {
		return 0, fmt.Errorf("vector dimensions mismatch: %d vs %d", a.Dimensions, b.Dimensions)
	}

	var dotProduct, normA, normB float64

	for i := 0; i < a.Dimensions; i++ {
		dotProduct += float64(a.Values[i]) * float64(b.Values[i])
		normA += float64(a.Values[i]) * float64(a.Values[i])
		normB += float64(b.Values[i]) * float64(b.Values[i])
	}

	normA = math.Sqrt(normA)
	normB = math.Sqrt(normB)

	if normA == 0 || normB == 0 {
		return 0, nil
	}

	return dotProduct / (normA * normB), nil
}

// CosineDistance calculates cosine distance (1 - cosine similarity)
// Returns a value between 0 and 2 (0 = identical, 2 = opposite)
func CosineDistance(a, b *Vector) (float64, error) {
	similarity, err := CosineSimilarity(a, b)
	if err != nil {
		return 0, err
	}
	return 1 - similarity, nil
}

// L2Distance calculates Euclidean (L2) distance between two vectors
func L2Distance(a, b *Vector) (float64, error) {
	if a == nil || b == nil {
		return 0, fmt.Errorf("vectors cannot be nil")
	}

	if a.Dimensions != b.Dimensions {
		return 0, fmt.Errorf("vector dimensions mismatch: %d vs %d", a.Dimensions, b.Dimensions)
	}

	var sum float64
	for i := 0; i < a.Dimensions; i++ {
		diff := float64(a.Values[i]) - float64(b.Values[i])
		sum += diff * diff
	}

	return math.Sqrt(sum), nil
}

// InnerProduct calculates dot product of two vectors
func InnerProduct(a, b *Vector) (float64, error) {
	if a == nil || b == nil {
		return 0, fmt.Errorf("vectors cannot be nil")
	}

	if a.Dimensions != b.Dimensions {
		return 0, fmt.Errorf("vector dimensions mismatch: %d vs %d", a.Dimensions, b.Dimensions)
	}

	var product float64
	for i := 0; i < a.Dimensions; i++ {
		product += float64(a.Values[i]) * float64(b.Values[i])
	}

	return product, nil
}

// VectorSearchResult represents a search result with distance
type VectorSearchResult struct {
	Row      Row
	Distance float64
}

// VectorSearch performs similarity search on a table
func VectorSearch(rows []Row, queryVector *Vector, vectorColumn string, metric VectorDistance, limit int) ([]VectorSearchResult, error) {
	results := make([]VectorSearchResult, 0, len(rows))

	for _, row := range rows {
		val, exists := row[vectorColumn]
		if !exists {
			continue
		}

		rowVector, ok := val.(*Vector)
		if !ok {
			continue
		}

		distance, err := CalculateDistance(queryVector, rowVector, metric)
		if err != nil {
			continue
		}

		results = append(results, VectorSearchResult{
			Row:      row,
			Distance: distance,
		})
	}

	// Sort by distance (ascending - closest first)
	for i := 0; i < len(results)-1; i++ {
		for j := i + 1; j < len(results); j++ {
			if results[j].Distance < results[i].Distance {
				results[i], results[j] = results[j], results[i]
			}
		}
	}

	// Apply limit
	if limit > 0 && limit < len(results) {
		results = results[:limit]
	}

	return results, nil
}

// ParseVector parses a vector from string representation
// Format: [0.1, 0.2, 0.3] or ARRAY[0.1, 0.2, 0.3]
func ParseVector(s string) (*Vector, error) {
	// Remove ARRAY prefix if present
	if len(s) > 6 && s[:5] == "ARRAY" {
		s = s[5:]
	}

	// Remove brackets
	if len(s) < 2 || s[0] != '[' || s[len(s)-1] != ']' {
		return nil, fmt.Errorf("invalid vector format: must be [v1, v2, ...]")
	}
	s = s[1 : len(s)-1]

	// Split by comma
	parts := splitVector(s)
	values := make([]float32, len(parts))

	for i, part := range parts {
		var val float64
		_, err := fmt.Sscanf(part, "%f", &val)
		if err != nil {
			return nil, fmt.Errorf("invalid vector value: %s", part)
		}
		values[i] = float32(val)
	}

	return NewVector(values), nil
}

func splitVector(s string) []string {
	var parts []string
	var current string

	for _, ch := range s {
		if ch == ',' {
			if current != "" {
				parts = append(parts, current)
				current = ""
			}
		} else if ch != ' ' && ch != '\t' && ch != '\n' {
			current += string(ch)
		}
	}

	if current != "" {
		parts = append(parts, current)
	}

	return parts
}

// Update CalculateDistance to check dimensions
func CalculateDistance(a, b *Vector, metric VectorDistance) (float64, error) {
	if a == nil || b == nil {
		return 0, fmt.Errorf("vectors cannot be nil")
	}

	if a.Dimensions != b.Dimensions {
		return 0, fmt.Errorf("vector dimension mismatch: got %d, expected %d", b.Dimensions, a.Dimensions)
	}

	switch metric {
	case DistanceCosine:
		return CosineDistance(a, b)
	case DistanceL2:
		return L2Distance(a, b)
	case DistanceInnerProd:
		prod, err := InnerProduct(a, b)
		return -prod, err
	default:
		return 0, fmt.Errorf("unsupported distance metric: %s", metric)
	}
}
