

1. Modfy the query in prepareNewPartitions -> Else part to use "left" function rather than duing a fixed position like : 10)::date 
```
SELECT max(left(substring(pg_catalog.pg_get_expr(c.relpartbound, c.oid),position('TO (' IN pg_catalog.pg_get_expr(c.relpartbound, c.oid))+5),-2)::timestamp)
         FROM pg_catalog.pg_class c
         join pg_catalog.pg_inherits i on c.oid=i.inhrelid
         WHERE i.inhparent = 91972;
```
2. Instead of ::date use actual datatype
2. Instead of ::date use actual datatype

