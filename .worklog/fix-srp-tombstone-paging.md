# Fix SRP tombstone paging

- Failed: Replica tombstone paging made an incomplete source response appear
  exhausted, and a late local read could restore an old checkpoint.
- Learned: The coordinator needs explicit response metadata and explicit pager
  ownership because the replica command cursor does not cross the message
  boundary.
- Fixed: Responses carry checkpoint counts, supported short reads continue,
  unsupported paths fail safely, and read repair transfers ownership before it
  dispatches full-data reads.
