package pg

// PostgreSQL Message Types
const (
	MsgStartup         = 0  // Not a single byte char
	MsgSSLRequest      = 0  // Special code
	MsgPassword        = 'p'
	MsgQuery           = 'Q'
	MsgTerminate       = 'X'
	MsgParse           = 'P'
	MsgBind            = 'B'
	MsgDescribe        = 'D'
	MsgExecute         = 'E'
	MsgSync            = 'S'
	MsgFlush           = 'H'
)

// PostgreSQL Backend Response Types
const (
	ResAuthentication  = 'R'
	ResBackendKeyData  = 'K'
	ResParameterStatus = 'S'
	ResReadyForQuery   = 'Z'
	ResRowDescription  = 'T'
	ResDataRow         = 'D'
	ResCommandComplete = 'C'
	ResErrorResponse   = 'E'
	ResNoticeResponse  = 'N'
)

// PostgreSQL Type OIDs (Object Identifiers)
const (
	OIDInt4    = 23
	OIDText    = 25
	OIDFloat8  = 701
	OIDBool    = 16
	OIDVarchar = 1043
)
