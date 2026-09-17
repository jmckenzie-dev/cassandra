# Fix virtual-table threshold read dispatch

## Cause

`SinglePartitionReadQuery.Group` and pagers call
`executeLocally(controller, tombstonePagingThreshold)`. The virtual-table
single-partition and range commands override only `executeLocally(controller)`.
Java dispatch therefore selects the physical-table implementation inherited
from `ReadCommand`. That implementation calls `Keyspace.openAndGetStore` and
fails because virtual keyspaces do not exist in the normal keyspace registry.

## Plan

1. Override the threshold-aware method in both virtual command types.
2. Delegate to the existing virtual-table method. Virtual tables do not expose
   storage tombstones, so the threshold does not change the selected data.
3. Run `VirtualTableTest`, which already covers grouped internal reads, network
   reads, and paged virtual reads.
4. Run the read-command verb-handler tests to confirm the overload correction
   in their command doubles and the request-routing path.
5. Run the relevant paging property tests if the direct checks pass.

## Preserved behavior

- Keep virtual-table selection, row filtering, and limits unchanged.
- Keep the legacy overload for existing callers.
- Add no state, allocation, synchronization, or public API.
