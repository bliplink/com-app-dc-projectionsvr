# ProjectionSvr

`ProjectionSvr` is the MySQL read-model writer for the SaaS order cluster.
OrderSvr A/B recover exclusively from their replicated local journal and
snapshots. ProjectionSvr consumes only commit-proven state records and updates
`dc_orders`, `dc_orders_execorders`, the projection event ledger and per-partition
watermarks transactionally.

MySQL is a query/audit projection and is never an OrderSvr recovery source.
