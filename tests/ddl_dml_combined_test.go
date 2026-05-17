package tests

import "testing"

func TestDDLDMLGaps(t *testing.T) {
	t.Run("DDL_Gaps", TestDDLGaps)
	t.Run("DML_Gaps", TestDMLGaps)
}
