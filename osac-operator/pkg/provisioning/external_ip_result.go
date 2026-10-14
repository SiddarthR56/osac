/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package provisioning

import (
	"encoding/json"
	"fmt"
	"math"
)

// ExternalIPOperation identifies a normalized ExternalIP manager operation.
type ExternalIPOperation string

const (
	ExternalIPOperationAllocate ExternalIPOperation = "external_ip.allocate"
	ExternalIPOperationRelease  ExternalIPOperation = "external_ip.release"
)

// ExternalIPOutcome is the stable result consumed by ExternalIP lifecycle logic.
// Wire-format details stay in ParseExternalIPResult so the manager result can
// evolve without changing status or finalizer behavior.
type ExternalIPOutcome struct {
	Operation   ExternalIPOperation
	ResourceUID string
	Generation  int64
}

// ParseExternalIPResult translates the current AAP artifact envelope into the
// stable ExternalIP outcome contract. The AAP job status itself is checked by
// the provider before callers accept this result.
func ParseExternalIPResult(artifacts map[string]any) (ExternalIPOutcome, error) {
	if artifacts == nil {
		return ExternalIPOutcome{}, fmt.Errorf("AAP artifacts are missing")
	}
	wireValue, ok := artifacts["osac_result"]
	if !ok {
		return ExternalIPOutcome{}, fmt.Errorf("AAP artifacts do not contain osac_result")
	}
	wire, ok := wireValue.(map[string]any)
	if !ok {
		return ExternalIPOutcome{}, fmt.Errorf("AAP osac_result must be an object")
	}
	version, ok := wire["schemaVersion"].(string)
	if !ok || version != "v1" {
		return ExternalIPOutcome{}, fmt.Errorf("unsupported or missing osac_result schemaVersion")
	}
	operationText, ok := wire["operation"].(string)
	if !ok {
		return ExternalIPOutcome{}, fmt.Errorf("osac_result operation must be a string")
	}
	operation := ExternalIPOperation(operationText)
	if operation != ExternalIPOperationAllocate && operation != ExternalIPOperationRelease {
		return ExternalIPOutcome{}, fmt.Errorf("unsupported osac_result operation %q", operationText)
	}
	resourceUID, ok := wire["resourceUID"].(string)
	if !ok || resourceUID == "" {
		return ExternalIPOutcome{}, fmt.Errorf("osac_result resourceUID must be a non-empty string")
	}
	generation, err := parseExternalIPResultGeneration(wire["generation"])
	if err != nil {
		return ExternalIPOutcome{}, err
	}
	data, ok := wire["data"].(map[string]any)
	if !ok {
		return ExternalIPOutcome{}, fmt.Errorf("osac_result data must be an object")
	}
	if len(data) != 0 {
		return ExternalIPOutcome{}, fmt.Errorf("osac_result data must be empty for ExternalIP allocate/release")
	}

	return ExternalIPOutcome{
		Operation:   operation,
		ResourceUID: resourceUID,
		Generation:  generation,
	}, nil
}

func parseExternalIPResultGeneration(value any) (int64, error) {
	var generation int64
	switch typed := value.(type) {
	case int:
		generation = int64(typed)
	case int32:
		generation = int64(typed)
	case int64:
		generation = typed
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return 0, fmt.Errorf("osac_result generation is out of range")
		}
		generation = int64(typed)
	case uint64:
		if typed > math.MaxInt64 {
			return 0, fmt.Errorf("osac_result generation is out of range")
		}
		generation = int64(typed)
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) || math.Trunc(typed) != typed || typed > math.MaxInt64 || typed < math.MinInt64 {
			return 0, fmt.Errorf("osac_result generation must be an integer")
		}
		generation = int64(typed)
	case json.Number:
		parsed, err := typed.Int64()
		if err != nil {
			return 0, fmt.Errorf("osac_result generation must be an integer: %w", err)
		}
		generation = parsed
	default:
		return 0, fmt.Errorf("osac_result generation must be a number")
	}
	if generation < 0 {
		return 0, fmt.Errorf("osac_result generation must not be negative")
	}
	return generation, nil
}
