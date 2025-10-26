Table Storage:
┌─────────────────────────────────────┐
│ Table Header Page (Page 0)          │
│ - Magic number                      │
│ - Table schema                      │
│ - Root B+tree page ID               │
│ - Row count, statistics             │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ B+Tree Index Pages                  │
│ - Internal nodes (keys + page IDs)  │
│ - Leaf nodes (keys + row data)      │
└─────────────────────────────────────┘
           ↓
┌─────────────────────────────────────┐
│ Data Pages                          │
│ - Fixed/variable length rows        │
│ - Slotted page format               │
└─────────────────────────────────────┘